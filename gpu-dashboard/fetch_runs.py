from dataclasses import asdict, dataclass
import datetime as dt
from fnmatch import fnmatch
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
                    heartbeatAt
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
    run_path: str
    created_at: dt.datetime
    updated_at: dt.datetime
    state: str
    tags: list[str]
    host_name: str
    gpu_name: str
    gpu_count: int
    metrics_df: pl.DataFrame = None

    def get_team(self) -> str:
        team = self.run_path.split("/")[0]
        return team

    def get_project(self) -> str:
        project = self.run_path.split("/")[1]
        return project

    def get_run_id(self) -> str:
        run_id = self.run_path.split("/")[2]
        return run_id
    
    def to_log_dict(self):
        ignore_keys = ["metrics_df"]
        return {k: v for k, v in asdict(self).items() if k not in ignore_keys}

@dataclass
class Project:
    project: str
    runs: list[Run] = None


@dataclass
class Tree:
    team: str
    start_date: dt.date
    end_date: dt.date
    distributed_learning: bool
    ignore_project_pattern: str = None
    projects: list[Project] = None


def fetch_runs(target_date: dt.datetime) -> pl.DataFrame:
    """新しいrunを取得し、データを整形して返す"""
    trees = plant_trees()

    print("Get projects for each team ...")
    for tree in trees:
        projects = []
        if (target_date >= tree.start_date) & (target_date < tree.end_date):
            for p in wandb.Api().projects(tree.team):
                if tree.ignore_project_pattern is not None:
                    if fnmatch(p.name, tree.ignore_project_pattern):
                        continue
                projects.append(Project(project=p.name))
        else:
            print(f"  {tree.team}: Not started yet or already ended.")
        tree.projects = projects

    print("Get runs for each project ...")
    for tree in tqdm(trees):
        print("  Team:", tree.team)
        for project in tqdm(tree.projects):
            runs = query_runs(
                team=tree.team,
                project=project.project,
                target_date=target_date,
                distributed_learning=tree.distributed_learning,
            )
            project.runs = runs

    print("Checking overlap of runs in each team ...")
    overlap_run_pairs = []
    for tree in tqdm(trees):
        print("  Team:", tree.team)
        team_runs = []
        for project in tqdm(tree.projects):
            for run in project.runs:
                team_runs.append(run)
        overlap_run_pairs += find_overlap_run_pairs(runs=team_runs)
    alert_overlap_runs(overlap_run_pairs=overlap_run_pairs)

    print("Get metrics for each run ...")
    for tree in tqdm(trees):
        print("  Team:", tree.team)
        for project in tqdm(tree.projects):
            for run in project.runs:
                metrics_df = get_metrics(
                    run_path=run.run_path,
                    target_date=target_date,
                )
                run.metrics_df = metrics_df

    # concat run df
    df_list = []
    for tree in tqdm(trees):
        print("  Team:", tree.team)
        for project in tree.projects:
            for run in project.runs:
                new_run_df = get_new_run_df(
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

    def get_end_date(company_schedule: list[EasyDict]) -> dt.date:
        """GPU割り当て終了日を取得する"""
        df = pl.DataFrame(company_schedule).with_columns(
            pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d").cast(pl.Date),
        )
        last_row = df.sort("date")[-1]
        if last_row["assigned_gpu_node"].item() == 0:
            return last_row["date"].item()
        else:
            return dt.date(2100, 1, 1)

    trees = []
    for comp in CONFIG.companies:
        for team in comp.teams:
            tree = Tree(
                team=team,
                start_date=get_start_date(company_schedule=comp.schedule),
                end_date=get_end_date(company_schedule=comp.schedule),
                ignore_project_pattern=comp.get("ignore_project_pattern", None),
                distributed_learning=comp.get("distributed_learning", False),
            )
            trees.append(tree)
    return trees


def query_runs(
    team: str,
    project: str,
    target_date: dt.date,
    distributed_learning: bool,
    make_blacklist: bool = False,
) -> list[Run]:
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
        updatedAt = dt.datetime.fromisoformat(node.heartbeatAt) + dt.timedelta(
            hours=JAPAN_UTC_OFFSET
        )

        # Skip
        if not node.get("runInfo"):
            continue
        if not node.get("runInfo").get("gpu"):
            continue
        if createdAt.timestamp() == updatedAt.timestamp():  # 即終了したもの
            continue
        if not make_blacklist:
            if CONFIG.ignore_tag in [
                t.lower() for t in node.tags
            ]:  # 特定のtagをスキップ
                continue
        if target_date is not None:
            if target_date > updatedAt.date():  # 昨日以前に終了したものはスキップ
                continue
            if target_date < createdAt.date():  # 未来のものはスキップ
                continue

        run_path = "/".join((team, project, node.name))

        # 分散学習の場合はworld_sizeをgpu countとして扱う
        world_size = get_world_size(run_path=run_path)
        if distributed_learning and world_size > 0:
            gpu_count = world_size
        else:
            gpu_count = node.runInfo.gpuCount

        # データ追加
        run = Run(
            run_path=run_path,
            updated_at=updatedAt,
            created_at=createdAt,
            state=node.state,
            tags=node.tags,
            host_name=node.host,
            gpu_name=node.runInfo.gpu,
            gpu_count=gpu_count,
        )
        runs.append(run)
    return runs


def find_overlap_run_pairs(runs: list[Run]) -> list[tuple[Run]]:
    """team内で同じhostが同時にrunを作っているものを検出する"""
    overlap_run_pairs = []
    for i in range(len(runs)):
        for j in range(i + 1, len(runs)):
            if (
                runs[i].host_name == runs[j].host_name
                and runs[i].created_at < runs[j].updated_at
                and runs[j].created_at < runs[i].updated_at
            ):
                overlap_run_pairs.append((runs[i], runs[j]))
    return overlap_run_pairs


def alert_overlap_runs(overlap_run_pairs: list[tuple[Run]]) -> None:
    if not CONFIG.enable_alert:
        return None
    with wandb.init(
        entity=CONFIG.dashboard.entity,
        project=CONFIG.dashboard.project,
        name=f"Overlap Alert",
    ) as run:
        if overlap_run_pairs:
            for run_pair in overlap_run_pairs:
                run_pair_str = ", ".join(f"{run.to_log_dict()}" for run in run_pair)
                print(run_pair_str)
            teams = sorted(set(c[0].get_team() for c in overlap_run_pairs))
            wandb.alert(title="Overlap of runs found", text="\n".join(teams))
        else:
            print("No overlaps found.")
    return None


def get_metrics(
    run_path: str,
    target_date: dt.date,
) -> pl.DataFrame:
    # raw data
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


def get_new_run_df(run: Run, target_date: dt.date) -> pl.DataFrame:
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
        pl.lit(run.get_team()).cast(pl.String).alias("company_name"),
        pl.lit(run.get_project()).cast(pl.String).alias("project"),
        pl.lit(run.get_run_id()).cast(pl.String).alias("run_id"),
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


def get_world_size(run_path: str) -> int:
    api = wandb.Api()
    run = api.run(run_path)
    config = run.config
    world_size = config.get("world_size", 0)
    return world_size
