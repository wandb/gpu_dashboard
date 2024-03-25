from dataclasses import dataclass
import datetime as dt
import pytz
import re

from easydict import EasyDict
import polars as pl
from tqdm import tqdm
import wandb
from wandb_gql import gql

from config import CONFIG

JAPAN_TIMEZONE = pytz.timezone("Asia/Tokyo")
LOGGED_AT = dt.datetime.now(JAPAN_TIMEZONE).replace(tzinfo=None)
JAPAN_UTC_OFFSET = 9

GQL_QUERY = """\
query GetGpuInfoForProject($project: String!, $entity: String!, $first: Int!, $cursor: String!) {
    project(name: $project, entityName: $entity) {
        name
        runs(first: $first, after: $cursor) {
            edges {
                cursor
                node {
                    name
                    createdAt
                    updatedAt
                    state
                    tags
                    host
                    runInfo {
                        gpuCount
                        gpu
                    }
                }
            }
        }
    }
}\
"""


@dataclass
class Run:
    run_id: str
    run_path: str
    created_at: dt.datetime
    updated_at: dt.datetime
    state: str
    tags: list[str]
    host_name: str
    gpu_name: str
    gpu_count: int
    metrics_df: pl.DataFrame = None


@dataclass
class Project:
    project: str
    runs: list[Run] = None


@dataclass
class Tree:
    team: str
    start_date: dt.date
    ignore_projects: list[str]
    projects: list[Project] = None
    runs: list[Run] = None


def fetch_runs(target_date: dt.datetime):
    """新しいrunを取得し、データを整形して返す"""
    trees = plant_trees()

    print("Get projects for each team ...")
    for tree in trees:
        if target_date >= tree.start_date:
            projects = [
                Project(project=p.name)
                for p in wandb.Api().projects(tree.team)
                if p.name not in tree.ignore_projects
            ]
        else:
            projects = []
        tree.projects = projects

    print("Get runs for each project ...")
    for tree in tqdm(trees):
        print("Team:", tree.team)
        for project in tqdm(tree.projects):
            runs = query_runs(
                team=tree.team,
                project=project.project,
                target_date=target_date,
            )
            project.runs = runs

    print("Checking overlap of runs in each team ...")
    alert_texts = []
    for tree in tqdm(trees):
        print("Team:", tree.team)
        team_runs = []
        for project in tqdm(tree.projects):
            for run in project.runs:
                team_runs.append(run)
        alert_texts += find_overlap_runs(runs=team_runs)
    alert_overlap_runs(alert_texts=alert_texts)

    print("Get metrics for each run ...")
    for tree in tqdm(trees):
        print("Team:", tree.team)
        for project in tqdm(tree.projects):
            for run in project.runs:
                metrics_df = get_metrics(
                    team=tree.team,
                    project=project.project,
                    run_id=run.run_id,
                    target_date=target_date,
                )
                run.metrics_df = metrics_df

    # concat run df
    df_list = []
    for tree in tqdm(trees):
        print("Team:", tree.team)
        for project in tree.projects:
            for run in project.runs:
                new_run_df = get_new_run_df(
                    team=tree.team,
                    project=project.project,
                    run=run,
                    target_date=target_date,
                )
                df_list.append(new_run_df)
    new_runs_df = pl.concat(df_list)
    return new_runs_df


def plant_trees() -> list[Tree]:
    """run取得に必要なデータを持つTreeオブジェクトを作成する"""

    def get_start_date(company_schedule: list[EasyDict]) -> dt.date:
        """GPU割り当て開始日を取得する"""
        df = pl.DataFrame(company_schedule).with_columns(
            pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d").cast(pl.Date),
        )
        start_date = df["date"].min()
        return start_date

    trees = []
    for comp in CONFIG.companies:
        for team in comp.teams:
            tree = Tree(
                team=team,
                start_date=get_start_date(comp.schedule),
                ignore_projects=comp.get("ignore_projects", ""),
            )
            trees.append(tree)
    return trees


def query_runs(team: str, project: str, target_date: dt.date) -> list[Run]:
    if CONFIG.testmode:
        if team != "stockmark-geniac":
            return []
    api = wandb.Api(timeout=60)
    cursor = ""
    nodes, runs = [], []
    while True:
        results = api.client.execute(
            gql(GQL_QUERY),
            {
                "entity": team,
                "project": project,
                "first": 1000,
                "cursor": cursor,
            },
        )
        _edges = results["project"]["runs"]["edges"]
        if not _edges:
            break
        nodes += [EasyDict(e["node"]) for e in _edges]
        cursor = _edges[-1]["cursor"]
    for node in nodes:
        # 日付で時差を考慮
        createdAt = dt.datetime.fromisoformat(node.createdAt) + dt.timedelta(
            hours=JAPAN_UTC_OFFSET
        )
        updatedAt = dt.datetime.fromisoformat(node.updatedAt) + dt.timedelta(
            hours=JAPAN_UTC_OFFSET
        )

        # Skip
        if not node.get("runInfo"):
            continue
        if not node.get("runInfo").get("gpu"):
            continue
        if createdAt.timestamp() == updatedAt.timestamp():  # 即終了したもの
            continue
        if target_date > updatedAt.date():  # 昨日以前に終了したものはスキップ
            continue
        if target_date < createdAt.date():  # 未来のものはスキップ
            continue
        if CONFIG.ignore_tag in node.tags:  # 特定のtagをスキップ
            continue

        # データ追加
        run = Run(
            run_id=node.name,
            run_path=("/").join((team, project, node.name)),
            updated_at=updatedAt,
            created_at=createdAt,
            state=node.state,
            tags=node.tags,
            host_name=node.host,
            gpu_name=node.runInfo.gpu,
            gpu_count=node.runInfo.gpuCount,
        )
        runs.append(run)
    return runs


def find_overlap_runs(runs: list[Run]) -> list[str]:
    """team内で同じhostが同時にrunを作っているものを"""
    alert_texts = []
    for i in range(len(runs)):
        for j in range(i + 1, len(runs)):
            if (
                runs[i].host_name == runs[j].host_name
                and runs[i].created_at < runs[j].updated_at
                and runs[j].created_at < runs[i].updated_at
            ):
                alert_text = "Overlap of runs found. Please check runs below.\n  {run1}\n  {run2}".format(
                    run1=runs[i].__dict__, run2=runs[j].__dict__
                )
                print(alert_text)
                alert_texts.append(alert_text)
    return alert_texts


def alert_overlap_runs(alert_texts: list[str]) -> None:
    if (not alert_texts) | CONFIG.testmode:
        return None
    with wandb.init(
        entity=CONFIG.dashboard.entity,
        project=CONFIG.dashboard.project,
        name=f"Alert",
    ) as run:
        for alert_text in alert_texts:
            wandb.alert(alert_text)
    return None


def get_metrics(
    team: str,
    project: str,
    run_id: str,
    target_date: dt.date,
) -> pl.DataFrame:
    # raw data
    run_path = ("/").join((team, project, run_id))
    api = wandb.Api()
    run = api.run(path=run_path)
    metrics_df = pl.from_dataframe(run.history(stream="events", samples=100))
    # filter
    if len(metrics_df) <= 1:
        return pl.DataFrame()
    metrics_df_with_datetime = (
        metrics_df
        ## cast to datetime
        .with_columns(
            pl.col("_timestamp")
            .map_elements(lambda x: dt.datetime.fromtimestamp(x))
            .alias("datetime")
            ## filter by target_date
        ).filter(
            pl.col("datetime")
            <= dt.datetime.combine(target_date + dt.timedelta(days=1), dt.time())
        )
    )
    if metrics_df_with_datetime.is_empty():
        return pl.DataFrame()
    # process
    daily_metrics_df = (
        metrics_df_with_datetime.lazy()
        # 縦持ちに変換
        .select(
            "datetime",
            "_timestamp",
            gpu_ptn := ("^system\.gpu\.\d+\.gpu$"),
            memory_ptn := ("^system\.gpu\.\d+\.memory$"),
        )
        .with_columns(pl.col("datetime").cast(pl.Date).alias("date"))
        .melt(
            id_vars=["date", "datetime", "_timestamp"],
            value_vars=[c for c in metrics_df.columns if re.findall(gpu_ptn, c)]
            + [c for c in metrics_df.columns if re.findall(memory_ptn, c)],
            variable_name="gpu",
            value_name="value",
        )
        .with_columns(pl.col("gpu").map_elements(lambda x: x.split(".")[-1]))
        # 日付で集計
        .group_by(["date", "gpu"])
        .agg(
            pl.col("value").mean().alias("average"),
            pl.col("value").max().alias("max"),
            pl.col("_timestamp")
            .map_elements(
                lambda x: (max(x) - min(x)) / 60**2
            )  # seconds * 60 * 60 = hours
            .alias("metrics_hours"),
        )
        .collect()
        # 横持ちに変換
        .pivot(index="date", columns="gpu", values=["average", "max"])
        .rename(
            {
                f"{prefix}_gpu_gpu": f"{prefix}_gpu_utilization"
                for prefix in ("average", "max")
            }
        )
        .select(
            pl.col("date").cast(pl.Date),
            pl.col("average_gpu_utilization").cast(pl.Float64),
            pl.col("max_gpu_utilization").cast(pl.Float64),
            pl.col("average_gpu_memory").cast(pl.Float64),
            pl.col("max_gpu_memory").cast(pl.Float64),
        )
    )
    return daily_metrics_df


def get_new_run_df(
    team: str, project: str, run: Run, target_date: dt.date
) -> pl.DataFrame:
    def divide_duration_daily(
        start: dt.datetime,
        end: dt.datetime,
        target_date: dt.date,
    ) -> pl.DataFrame:
        """開始時間と終了時間から日ごとの経過時間のdfを作る"""
        minutes_range = (
            pl.datetime_range(
                start=start,
                end=end,
                interval="1m",
                eager=True,
            )
            .dt.strftime("%Y-%m-%d %H:00")  # secondsは無視
            .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M")  # datetime型に戻す
        )
        df = (
            pl.DataFrame()
            .with_columns(
                minutes_range.alias("datetime_mins"),
            )
            .with_columns(pl.col("datetime_mins").dt.strftime("%Y-%m-%d").alias("date"))
            .group_by("date")
            .agg(
                pl.col("datetime_mins").count().truediv(60).alias("duration_hour")
            )  # mins / 60 = hours
            .with_columns(
                pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d").cast(pl.Date),
            )
            .filter((pl.col("date") <= target_date))
            .select(
                pl.col("date").cast(pl.Date),
                pl.col("duration_hour").cast(pl.Float64),
            )
        )
        return df

    duration_df = divide_duration_daily(
        start=run.created_at,
        end=run.updated_at,
        target_date=target_date,
    )
    if run.metrics_df.is_empty():
        new_run_df = duration_df.with_columns(
            pl.lit(None).cast(pl.Float64).alias("average_gpu_utilization"),
            pl.lit(None).cast(pl.Float64).alias("max_gpu_utilization"),
            pl.lit(None).cast(pl.Float64).alias("average_gpu_memory"),
            pl.lit(None).cast(pl.Float64).alias("max_gpu_memory"),
        )
    else:
        new_run_df = duration_df.join(run.metrics_df, on=["date"], how="left")
    new_run_df = new_run_df.with_columns(
        pl.lit(team).cast(pl.String).alias("company_name"),
        pl.lit(project).cast(pl.String).alias("project"),
        pl.lit(run.run_id).cast(pl.String).alias("run_id"),
        pl.lit(0).cast(pl.Int64).alias("assigned_gpu_node"),  # not in use
        pl.lit(run.created_at).cast(pl.Datetime).alias("created_at"),
        pl.lit(run.updated_at).cast(pl.Datetime).alias("updated_at"),
        pl.lit(run.state).cast(pl.String).alias("state"),
        pl.lit(run.gpu_count).cast(pl.Int64).alias("gpu_count"),
        pl.lit(LOGGED_AT).cast(pl.Datetime).alias("logged_at"),
        pl.lit(CONFIG.testmode).cast(pl.Boolean).alias("testmode"),
    ).select(
        "date",
        "company_name",
        "project",
        "run_id",
        "assigned_gpu_node",
        "created_at",
        "updated_at",
        "state",
        "duration_hour",
        "gpu_count",
        "average_gpu_utilization",
        "average_gpu_memory",
        "max_gpu_utilization",
        "max_gpu_memory",
        "logged_at",
        "testmode",
    )
    return new_run_df
