from dataclasses import dataclass
import datetime as dt
from pathlib import Path
import re

from easydict import EasyDict
import pandas as pd
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
        # if project_name == "gpu-dashboard":
        #     print(len(runs))
        #     exit()
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
                # gpu_name="",
                # gpu_count="",
                state=run.state,
                tags=run.tags,
            )
            runs_info.append(run_info)
    return runs_info


def get_gpu_schedule(
    gpu_schedule: list[EasyDict], target_date: dt.date
) -> pl.DataFrame:
    gpu_schedule_df = pl.DataFrame(gpu_schedule).with_columns(
        pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d").cast(pl.Date),
        pl.col("assigned_gpu_node").cast(pl.Float64),
    )
    date_df = pl.DataFrame(
        pl.datetime_range(
            start=min(gpu_schedule_df["date"]),
            end=target_date,
            interval="1d",
            eager=True,
        )
        .cast(pl.Date)
        .alias("date")
    )
    expanded_gpu_schedule_df = (
        date_df.join(gpu_schedule_df, on=["date"], how="left")
        .with_columns(pl.col("assigned_gpu_node").forward_fill())
        .select(
            pl.col("date").cast(pl.Date),
            pl.col("assigned_gpu_node").cast(pl.Float64),
        )
    )
    return expanded_gpu_schedule_df


def get_whole_gpu_schedule(
    companies_config: list[EasyDict], target_date: dt.date
) -> pl.DataFrame:
    df_list = []
    for company_config in companies_config:
        tmp_df = get_gpu_schedule(
            gpu_schedule=company_config.schedule, target_date=target_date
        ).with_columns(pl.lit(company_config.company_name).alias("company_name"))
        df_list.append(tmp_df)
    df = pl.concat(df_list)
    return df


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
        .select(
            pl.col("date").cast(pl.Date),
            pl.col("duration_hour").cast(pl.Float64),
        )
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
    metrics_df_with_datetime = metrics_df.with_columns(
        pl.col("_timestamp")
        .map_elements(lambda x: dt.datetime.fromtimestamp(x))
        .alias("datetime")
    ).filter(
        pl.col("datetime")
        <= dt.datetime.combine(target_date + dt.timedelta(days=1), dt.time())
    )
    if metrics_df_with_datetime.is_empty():
        return pl.DataFrame()
    daily_metrics_df = (
        metrics_df_with_datetime.lazy()
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
        .group_by(["date", "gpu"])
        .agg(
            pl.col("value").mean().alias("average"),
            pl.col("value").max().alias("max"),
            pl.col("_timestamp")
            .map_elements(lambda x: (max(x) - min(x)) / 60**2)
            .alias("metrics_hours"),
        )
        .collect()
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


def read_table_csv(run: object, wandb_dir: str, artifact_name: str) -> pl.DataFrame:
    artifact = run.use_artifact(f"{artifact_name}:latest")
    artifact.download(wandb_dir)
    csv_path = Path(f"{wandb_dir}/{artifact_name}.csv")
    df = pl.from_pandas(
        pd.read_csv(
            csv_path,
            parse_dates=["created_at", "updated_at", "logged_at"],
            date_format="ISO8601",
        )
    ).with_columns(
        pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d").cast(pl.Date),
        pl.col("created_at").cast(pl.Datetime("us")),
        pl.col("updated_at").cast(pl.Datetime("us")),
        pl.col("logged_at").cast(pl.Datetime("us")),
    )
    return df


def daily_summarize(df: pl.DataFrame) -> pl.DataFrame:
    metrics_duraion_df = (
        df.filter(pl.col("max_gpu_memory").is_not_null())
        .with_columns(
            (pl.col("average_gpu_utilization") * pl.col("duration_hour")).alias(
                "weighted_average_gpu_utilization"
            ),
            (pl.col("average_gpu_memory") * pl.col("duration_hour")).alias(
                "weighted_average_gpu_memory"
            ),
        )
        .group_by("date", "company_name")
        .agg(
            pl.col("duration_hour").sum().alias("metrics_duration_hour"),
            pl.col("weighted_average_gpu_utilization").sum(),
            pl.col("weighted_average_gpu_memory").sum(),
        )
        .with_columns(
            (
                pl.col("weighted_average_gpu_utilization")
                / pl.col("metrics_duration_hour")
            ).alias("average_gpu_utilization"),
            (
                pl.col("weighted_average_gpu_memory") / pl.col("metrics_duration_hour")
            ).alias("average_gpu_memory"),
        )
        .select(
            "date",
            "company_name",
            "average_gpu_utilization",
            "average_gpu_memory",
            "metrics_duration_hour",
        )
    )

    daily_summary_df = (
        df.with_columns(
            (pl.col("duration_hour") * pl.col("gpu_count")).alias("total_gpu_hour"),
        )
        .group_by("date", "company_name")
        .agg(
            pl.col("run_id").count().alias("n_runs").fill_null(0),
            pl.col("duration_hour").sum().fill_null(0),
            pl.col("total_gpu_hour").sum().fill_null(0),
            pl.col("assigned_gpu_node").max().fill_null(0),
            pl.col("max_gpu_utilization").max(),
            pl.col("max_gpu_memory").max(),
        )
        .with_columns(
            pl.col("assigned_gpu_node").mul(8).mul(24).alias("assigned_gpu_hour"),
        )
        .with_columns(
            (
                pl.col("total_gpu_hour").truediv(pl.col("assigned_gpu_hour")).mul(100)
            ).alias("utilization_rate"),
        )
        .join(metrics_duraion_df, on=["date", "company_name"])
        .sort(["date"], descending=True)
        .sort(["company_name"])
        .select(
            "date",  # key 1
            "company_name",  # key2
            "n_runs",
            "duration_hour",
            "total_gpu_hour",
            "utilization_rate",
            "average_gpu_utilization",
            "max_gpu_utilization",
            "average_gpu_memory",
            "max_gpu_memory",
        )
    )
    return daily_summary_df


def monthly_summarize(
    df: pl.DataFrame, companies_config: list[EasyDict], target_date: dt.date
) -> pl.DataFrame:
    ### Additional DataFrame
    # Get gpu schedule
    gpu_schedule_df = (
        get_whole_gpu_schedule(
            companies_config=companies_config, target_date=target_date
        )
        .with_columns(
            pl.col("date").dt.strftime("%Y-%m").alias("year_month"),
            pl.col("date").min().alias("start_date"),
        )
        .group_by("year_month", "company_name")
        .agg(
            pl.col("assigned_gpu_node").sum(),
            pl.col("date").count().alias("days"),
        ).with_columns(
            pl.col("assigned_gpu_node").mul(8 * 24).alias("assigned_gpu_hour"),
        )
    )

    # Add year_month
    df = df.with_columns(pl.col("date").dt.strftime("%Y-%m").alias("year_month"))
    # Prepare weighted average
    metrics_duraion_df = (
        df.filter(pl.col("max_gpu_memory").is_not_null())
        .with_columns(
            (pl.col("average_gpu_utilization") * pl.col("duration_hour")).alias(
                "weighted_average_gpu_utilization"
            ),
            (pl.col("average_gpu_memory") * pl.col("duration_hour")).alias(
                "weighted_average_gpu_memory"
            ),
        )
        .group_by("year_month", "company_name")
        .agg(
            pl.col("duration_hour").sum().alias("metrics_duration_hour"),
            pl.col("weighted_average_gpu_utilization").sum(),
            pl.col("weighted_average_gpu_memory").sum(),
        )
        .with_columns(
            (
                pl.col("weighted_average_gpu_utilization")
                / pl.col("metrics_duration_hour")
            ).alias("average_gpu_utilization"),
            (
                pl.col("weighted_average_gpu_memory") / pl.col("metrics_duration_hour")
            ).alias("average_gpu_memory"),
        )
        .select(
            "year_month",
            "company_name",
            "metrics_duration_hour",
            "average_gpu_utilization",
            "average_gpu_memory",
        )
    )

    # Aggregate
    monthly_summary_df = (
        df.filter(pl.col("date") <= target_date)
        .with_columns(
            (pl.col("duration_hour") * pl.col("gpu_count")).alias("total_gpu_hour"),
        )
        .group_by("year_month", "company_name")
        .agg(
            pl.col("run_id").n_unique().alias("n_runs").fill_null(0),
            pl.col("duration_hour").sum().fill_null(0),
            pl.col("total_gpu_hour").sum().fill_null(0),
            pl.col("max_gpu_utilization").max(),
            pl.col("max_gpu_memory").max(),
        )
        .join(gpu_schedule_df, on=["year_month", "company_name"], how="left")
        .join(metrics_duraion_df, on=["year_month", "company_name"])
        .with_columns(
            (
                pl.col("total_gpu_hour").truediv(pl.col("assigned_gpu_hour")).mul(100)
            ).alias("utilization_rate"),
        )
        .sort(["year_month"], descending=True)
        .sort(["company_name"])
        .select(
            "year_month",  # key 1
            "company_name",  # key2
            "days",
            "n_runs",
            "duration_hour",
            "total_gpu_hour",
            "assigned_gpu_node",
            "assigned_gpu_hour",
            "utilization_rate",
            "average_gpu_utilization",
            "max_gpu_utilization",
            "average_gpu_memory",
            "max_gpu_memory",
        )
    )

    return monthly_summary_df


def set_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Dataframeのdata型をcastする"""
    new_df = df.with_columns(
        pl.col("run_id").cast(pl.String),
        pl.col("assigned_gpu_node").cast(pl.Float64),
        pl.col("duration_hour").cast(pl.Float64),
        pl.col("gpu_count").cast(pl.Float64),
        pl.col("average_gpu_utilization").cast(pl.Float64),
        pl.col("average_gpu_memory").cast(pl.Float64),
        pl.col("max_gpu_utilization").cast(pl.Float64),
        pl.col("max_gpu_memory").cast(pl.Float64),
    )
    return new_df
