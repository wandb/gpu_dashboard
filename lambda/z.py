from dataclasses import dataclass
import datetime as dt
import re

from easydict import EasyDict
import polars as pl
import wandb
from wandb_gql import gql

### gqlのクエリ
QUERY = """\
query GetGpuInfoForProject($project: String!, $entity: String!) {
    project(name: $project, entityName: $entity) {
        name
        runs {
            edges {
                node {
                    name
                    user {
                        username
                    }
                    computeSeconds
                    createdAt
                    updatedAt
                    state
                    tags
                    systemMetrics
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
class RunInfo:
    company_name: str
    project: str
    run_id: str
    created_at: dt.datetime
    updated_at: dt.datetime
    username: str
    tags: list[str]
    gpu_name: str
    gpu_count: int
    state: str


def fetch_runs(
    company_name: str, target_date: dt.date, ignore_tag: str, testmode: bool
) -> list[RunInfo]:
    """entityごとにGPUを使用しているrunsのデータを取得する"""
    ### Query
    api = wandb.Api()
    project_names = [p.name for p in api.projects(company_name)]
    gpu_info_query = gql(QUERY)

    ### Process
    runs_info = []
    for project_name in project_names:
        if (testmode) & (len(runs_info) == 2):
            continue
        results = api.client.execute(
            gpu_info_query, {"project": project_name, "entity": company_name}
        )
        run_edges = results.get("project").get("runs").get("edges")
        runs = [EasyDict(e.get("node")) for e in run_edges]
        for run in runs:
            ### 日付
            createdAt = dt.datetime.fromisoformat(run.createdAt) + dt.timedelta(
                hours=9
            )  # 時差
            updatedAt = dt.datetime.fromisoformat(run.updatedAt) + dt.timedelta(
                hours=9
            )  # 時差

            ### Skip
            if not run.get("runInfo"):
                continue
            if not run.get("runInfo").get("gpu"):
                continue
            if createdAt.timestamp() == updatedAt.timestamp():
                # 即終了
                continue
            if target_date > updatedAt.date():
                # 昨日以前に終了したものはスキップ
                continue
            if target_date < createdAt.date():
                # 未来のものはスキップ
                continue
            if ignore_tag in run.tags:
                # 特定のtagをスキップ
                continue

            ### データ追加
            run_info = RunInfo(
                company_name=company_name,
                project=project_name,
                run_id=run.name,
                created_at=createdAt,
                updated_at=updatedAt,
                username=run.user.username,
                gpu_name=run.runInfo.gpu,
                gpu_count=run.runInfo.gpuCount,
                state=run.state,
                tags=run.tags,
            )
            runs_info.append(run_info)
    return runs_info


def get_gpu_schedule(
    gpu_schedule: list[EasyDict], target_date: dt.date
) -> pl.DataFrame:
    _gpu_schedule_df = pl.DataFrame(gpu_schedule).with_columns(
        pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d").cast(pl.Date),
        pl.col("assigned_gpu_node").cast(pl.Float64),
    )
    date_df = pl.DataFrame(
        pl.datetime_range(
            start=min(_gpu_schedule_df["date"]),
            end=target_date,
            interval="1d",
            eager=True,
        )
        .cast(pl.Date)
        .alias("date")
    )
    gpu_schedule_df = date_df.join(
        _gpu_schedule_df, on=["date"], how="left"
    ).with_columns(pl.col("assigned_gpu_node").forward_fill().cast(pl.Float64))
    return gpu_schedule_df


def divide_duration_daily(
    start: dt.datetime,
    end: dt.datetime,
    target_date: dt.date,
) -> pl.DataFrame:
    """開始時間と終了時間から日ごとの経過時間のdfを作る"""
    minutes_range = (
        pl.datetime_range(
            start=start,
            end=end + dt.timedelta(minutes=1),
            interval="1m",
            eager=True,
        )
        .dt.strftime("%Y-%m-%d %H:00")  # secondsは無視
        .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M")  # datetime型に戻す
    )
    df = (
        (
            pl.DataFrame()
            .with_columns(
                minutes_range.alias("datetime_mins"),
            )
            .with_columns(pl.col("datetime_mins").dt.strftime("%Y-%m-%d").alias("date"))
            .group_by("date")
            .agg(pl.col("datetime_mins").count().truediv(60).alias("duration_hour"))
        )
        .with_columns(
            pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d").cast(pl.Date),
        )
        .filter((pl.col("date") <= target_date))
        # .select(

        # )
    )
    return df

def get_metrics(
    target_date: dt.date,
    company_name: str,
    project: str,
    run_id: str,
) -> pl.DataFrame:
    """runごとのsystem metricsを日ごとに集計する"""
    ### Fetch
    run_path = ("/").join((company_name, project, run_id))
    api = wandb.Api()
    run = api.run(path=run_path)
    metrics_df = pl.from_dataframe(run.history(stream="events"))
    ### Process
    if len(metrics_df) <= 1:
        return pl.DataFrame()
    daily_metrics_df = (
        metrics_df.select(
            "_timestamp",
            gpu_ptn := ("^system\.gpu\.\d+\.gpu$"),
            memory_ptn := ("^system\.gpu\.\d+\.memory$"),
        )
        .with_columns(
            pl.col("_timestamp")
            .map_elements(lambda x: dt.datetime.fromtimestamp(x))
            .alias("datetime")
        )
        .filter(
            pl.col("datetime")
            <= dt.datetime.combine(target_date + dt.timedelta(days=1), dt.time())
        )
        .with_columns(pl.col("datetime").dt.date().alias("date"))
        .melt(
            id_vars=["date", "datetime", "_timestamp"],
            value_vars=[c for c in metrics_df.columns if re.findall(gpu_ptn, c)]
            + [c for c in metrics_df.columns if re.findall(memory_ptn, c)],
            variable_name="gpu",
            value_name="value",
        )
        .with_columns(pl.col("gpu").map_elements(lambda x: x.split(".")[-1]))
        .group_by(["date", "gpu"])
        .agg(
            pl.col("value").mean().alias("average"),
            pl.col("value").max().alias("max"),
            pl.col("_timestamp")
            .map_elements(lambda x: (max(x) - min(x)) / 60**2)
            .alias("metrics_hours"),
        )
        .pivot(index="date", columns="gpu", values=["average", "max"])
        .rename({f"{prefix}_gpu_gpu": f"{prefix}_gpu_utilization" for prefix in ("average", "max")})
        .select(
            "date",
            "average_gpu_utilization",
            "max_gpu_utilization",
            "average_gpu_memory",
            "max_gpu_memory",
        )
    )
    return daily_metrics_df
