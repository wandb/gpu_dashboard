import datetime as dt
from pathlib import Path

from easydict import EasyDict
import pandas as pd
import polars as pl
from tqdm import tqdm
import wandb

from z import (
    fetch_runs,
    get_gpu_schedule,
    divide_duration_daily,
    get_metrics,
    read_table_csv,
    daily_summarize,
    set_schema,
)


def pipeline(
    company_name: str,
    gpu_schedule: list[EasyDict],
    target_date: dt.date,
    logged_at: dt.datetime,
    ignore_tag: str,
    testmode: bool,
) -> pl.DataFrame:
    ### GPUスケジュール
    print(f"  Processing {company_name} ...")
    if target_date < min(pl.DataFrame(gpu_schedule)["date"].cast(pl.Date)):
        print("    Not started.")
        return pl.DataFrame()
    gpu_schedule_df: pl.Dataframe = get_gpu_schedule(
        gpu_schedule=gpu_schedule, target_date=target_date
    )
    ### Run取得
    runs_info = fetch_runs(
        company_name=company_name,
        target_date=target_date,
        ignore_tag=ignore_tag,
        testmode=testmode,
    )
    if not runs_info:
        print("  No runs found.")
        return pl.DataFrame()
    ### Process runs
    df_list = []
    for run_info in tqdm(runs_info):
        # Process each runs
        if (testmode) & (len(df_list) == 2):
            continue
        duration_df: pl.Dataframe = divide_duration_daily(
            start=run_info.created_at,
            end=run_info.updated_at,
            target_date=target_date,
        ).with_columns(
            pl.lit(run_info.company_name).cast(pl.String).alias("company_name"),
            pl.lit(run_info.project).cast(pl.String).alias("project"),
            pl.lit(run_info.run_id).cast(pl.String).alias("run_id"),
            pl.lit(run_info.created_at).cast(pl.Datetime).alias("created_at"),
            pl.lit(run_info.updated_at).cast(pl.Datetime).alias("updated_at"),
            pl.lit(run_info.state).cast(pl.String).alias("state"),
            pl.lit(run_info.gpu_count).cast(pl.Float64).alias("gpu_count"),
        )
        metrics_df: pl.Dataframe = get_metrics(
            target_date=target_date,
            company_name=run_info.company_name,
            project=run_info.project,
            run_id=run_info.run_id,
        )
        # Join
        _new_run_df: pl.Dataframe
        if metrics_df.is_empty():
            new_run_df = duration_df.with_columns(
                pl.lit(None).cast(pl.Float64).alias("average_gpu_utilization"),
                pl.lit(None).cast(pl.Float64).alias("max_gpu_utilization"),
                pl.lit(None).cast(pl.Float64).alias("average_gpu_memory"),
                pl.lit(None).cast(pl.Float64).alias("max_gpu_memory"),
            )
        else:
            new_run_df = duration_df.join(metrics_df, on=["date"], how="left")
        df_list.append(new_run_df)
    if df_list:
        new_runs_df: pl.Dataframe = (
            pl.concat(df_list)
            .join(gpu_schedule_df, on=["date"], how="left")
            .with_columns(
                pl.lit(logged_at).cast(pl.Datetime).alias("logged_at"),
                pl.lit(testmode).cast(bool).alias("testmode"),
            )
            .select(
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
        )
        return new_runs_df
    else:
        return pl.DataFrame()


def update_artifacts(
    new_runs_df: pl.DataFrame,
    target_date: dt.date,
    wandb_dir: str,
    path_to_dashboard: EasyDict,
    elapsed_time: str,
    testmode: bool,
) -> dict:
    """今日取得したrunと過去に取得したrunをconcatしてartifactsをupdateする"""
    target_date_str = target_date.strftime("%Y-%m-%d")
    with wandb.init(
        entity=path_to_dashboard.entity,
        project=path_to_dashboard.project,
        name=f"Update_{target_date_str}",
        job_type="update-datest",
    ) as run:
        csv_path = Path(f"/tmp/{path_to_dashboard.artifact_name}.csv")
        # 過去のrunの存在を確認
        exist = True
        try:
            artifact = run.use_artifact(f"{path_to_dashboard.artifact_name}:latest")
        except:
            exist = False
        if exist:
            # 過去のrunを取得
            old_runs_df = read_table_csv(
                run=run, wandb_dir=wandb_dir, artifact_name=path_to_dashboard.artifact_name
            )
            # concatして重複するrunを除外
            all_runs_df = (
                pl.concat((new_runs_df.pipe(set_schema), old_runs_df.pipe(set_schema)))
                .sort(["logged_at"], descending=True)
                .unique(["date", "company_name", "project", "run_id"], keep="first")
                .sort(["run_id", "project"])
                .sort(["date"], descending=True)
                .sort(["company_name"])
            )
            assert len(all_runs_df) >= len(
                old_runs_df
            ), f"!!! Data length error !!! all: {len(all_runs_df)}, old: {len(old_runs_df)}"
        else:
            if new_runs_df.is_empty():
                # Artifactが存在しなくて、新しいrunもない
                return {"message": "No runs found"}
            old_runs_df = pl.DataFrame()
            all_runs_df = new_runs_df.clone()
        # アーティファクト更新
        all_runs_df.write_csv(csv_path)
        artifact = wandb.Artifact(
            name=path_to_dashboard.artifact_name,
            type="dataset",
            metadata={
                "version": target_date_str,
                "elapsed_time": elapsed_time,
                "testmode": testmode,
            },
        )
        artifact.add_file(local_path=csv_path)
        run.log_artifact(artifact)
        new_records = len(all_runs_df) - len(old_runs_df)
        return new_records


def update_tables(
    wandb_dir: str, path_to_dashboard: EasyDict, target_date_str: str
) -> list[pl.DataFrame]:
    #Fetch csv
    with wandb.init(
        entity=path_to_dashboard.entity,
        project=path_to_dashboard.project,
        name=f"Read_{target_date_str}",
        job_type="read-table",
    ) as run:
        all_runs_df = read_table_csv(
            run=run, wandb_dir=wandb_dir, artifact_name=path_to_dashboard.artifact_name
        )
    # Daily summary
    daily_summary_df = daily_summarize(df=all_runs_df)
    for company_name in daily_summary_df["company_name"].unique():
        company_df = daily_summary_df.filter(pl.col("company_name") == company_name)
        with wandb.init(
            entity=path_to_dashboard.entity,
            project=path_to_dashboard.project,
            name=f"Tables_{target_date_str}",
            job_type="update-table",
            tags=[company_name, "latest"]
        ) as run:
            wandb.log({"company_daily_gpu_usage": wandb.Table(data=company_df.to_pandas())})
    # Overall summary
    return {}



# ### Utils
# def log_to_wandb(
#     path_to_dashboard: EasyDict,
#     run_name: str,
#     tables: dict[str, pl.DataFrame],
#     tags: list[str],
# ) -> None:
#     """Tableをwandbに出力する"""         # 時差を考慮
#     assert wandb.Api().default_entity == entity
#     config = dict(
#         entity=path_to_dashboard.entity,
#         project=path_to_dashboard.project,
#         name=run_name,
#         tags=tags,
#     )
#     with wandb.init(**config) as run:
#         for tbl_name, df in tables.items():
#             wandb.log({tbl_name: wandb.Table(data=df.to_pandas())})
#     return None


# def download_artifacts(path_to_dashboard: EasyDict) -> pl.DataFrame:
#     artifact = run.use_artifact(f"{path_to_dashboard.artifact_name}:latest")
#     artifact.download("/tmp")
#     csv_path = Path(f"/tmp/{path_to_dashboard.artifact_name}.csv")
#     old_runs_df = pl.from_pandas(
#         pd.read_csv(
#             csv_path,
#             parse_dates=["created_at", "updated_at", "logged_at"],
#             date_format="ISO8601",
#         )
#     ).with_columns(
#         pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d").cast(pl.Date),
#         pl.col("created_at").cast(pl.Datetime("us")),
#         pl.col("updated_at").cast(pl.Datetime("us")),
#         pl.col("logged_at").cast(pl.Datetime("us")),
#     )
#     return
