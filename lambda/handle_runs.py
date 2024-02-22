import datetime as dt
from typing import Any
from dataclasses import dataclass

import polars as pl
import re
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


def fetch_runs(company_name: str, target_date: dt.date = None) -> list[RunInfo]:
    """entityごとにGPUを使用しているrunsのデータを取得する"""
    # TODO dictからdataclassに変更
    api = wandb.Api()
    project_names = [p.name for p in api.projects(company_name)]
    gpu_info_query = gql(QUERY)

    runs_info = []
    for project_name in project_names:
        results = api.client.execute(
            gpu_info_query, {"project": project_name, "entity": company_name}
        )
        run_edges = results.get("project").get("runs").get("edges")
        runs = [e.get("node") for e in run_edges]

        for run in runs:
            # runInfoがなければスキップ
            if not run.get("runInfo"):
                continue
            # GPUなければスキップ
            if not run.get("runInfo").get("gpu"):
                continue

            # 日付
            createdAt = dt.datetime.fromisoformat(run["createdAt"])
            updatedAt = dt.datetime.fromisoformat(run["updatedAt"])

            # 即終了
            if updatedAt.timestamp() == createdAt.timestamp():
                continue

            # 過去に終了したものはスキップ
            if (target_date is not None) & (target_date > updatedAt.date()):
                continue

            # 未来のものはスキップ
            if (target_date is not None) & (target_date < createdAt.date()):
                continue

            runInfo = run["runInfo"]
            gpuName = runInfo["gpu"]
            gpuCount = runInfo["gpuCount"]

            # データ追加
            run_info = RunInfo(
                company_name=company_name,
                project=project_name,
                run_id=run["name"],
                created_at=createdAt,
                updated_at=updatedAt,
                username=run["user"]["username"],
                gpu_name=gpuName,
                gpu_count=gpuCount,
                state=run["state"],
                tags=run["tags"],
            )
            runs_info.append(run_info)
    return runs_info


def divide_duration_daily(start: dt.datetime, end: dt.datetime) -> pl.DataFrame:
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
        pl.DataFrame()
        .with_columns(
            minutes_range.alias("datetime_mins"),
        )
        .with_columns(pl.col("datetime_mins").dt.strftime("%Y-%m-%d").alias("date"))
        .group_by("date")
        .agg(pl.col("datetime_mins").count().truediv(60).alias("duration_hour"))
    )
    return df


def get_metrics_df(
    target_date: dt.date,
    company_name: str = None,
    project: str = None,
    run_id: str = None,
    run_path: str = None,
) -> pl.DataFrame:
    """runごとのsystem metricsを日ごとに集計する"""
    if run_path is None:
        run_path = ("/").join((company_name, project, run_id))
    api = wandb.Api()
    run = api.run(path=run_path)
    metrics_df = pl.from_dataframe(run.history(stream="events"))
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
            pl.col("datetime").cast(pl.Date) <= (target_date + dt.timedelta(days=1))
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
        .select(
            "date",
            "average_gpu_gpu",
            "max_gpu_gpu",
            "average_gpu_memory",
            "max_gpu_memory",
        )
    )
    return daily_metrics_df


def get_gpu_schedule(config: dict[str, Any], target_date: dt.date) -> pl.DataFrame:
    """日次のGPU割り当て数を取得する"""

    def get_start_date(company_cfg: dict[str, Any]) -> dt.date:
        "企業のGPU割り当て開始日を取得する"
        start_date: dt.date = min(
            dt.datetime.strptime(schedule["date"], "%Y-%m-%d").date()
            for schedule in company_cfg["schedule"]
        )
        return start_date

    df_list = []
    for company in config["companies"]:
        start_date: dt.date = get_start_date(company_cfg=company)
        date_df = pl.DataFrame(
            pl.datetime_range(
                start=start_date, end=target_date, interval="1d", eager=True
            )
            .dt.strftime("%Y-%m-%d")
            .alias("date")
        )
        gpu_df = date_df.join(
            pl.DataFrame(company["schedule"]), on="date", how="left"
        ).with_columns(
            pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d").cast(pl.Date),
            pl.col("assigned_gpu_node").forward_fill().cast(pl.Float64),
            pl.lit(company["company_name"]).alias("company_name"),
        )
        df_list.append(gpu_df)
    new_df = pl.concat(df_list)
    return new_df


def monthly_overall(df: pl.DataFrame) -> pl.DataFrame:
    """"""
    new_df = df.select(
        "company_name",
        "year_month",
        "total_gpu_hours",
        "average_gpu_utilization",
        "max_gpu_utilization",
        "average_gpu_memory",
        "max_gpu_memory",
        "assigned_gpu_hours",
        "utilization_rate",
    )
    return new_df


def overall(df: pl.DataFrame) -> pl.DataFrame:
    """"""
    new_df = df.select(
        "company_name",
        "assigned_gpu_hours",
        "total_gpu_hours",
        "average_gpu_utilization",
        "max_gpu_utilization",
        "average_gpu_memory",
        "max_gpu_memory",
        "utilization_rate",
    )
    return new_df
