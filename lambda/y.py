import datetime as dt
from pathlib import Path

from easydict import EasyDict
import pandas as pd
import polars as pl
from tqdm import tqdm
import wandb

from z import fetch_runs, get_gpu_schedule, divide_duration_daily, get_metrics
from utils import set_schema


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
    gpu_schedule_df = get_gpu_schedule(
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
        duration_df = divide_duration_daily(
            start=run_info.created_at,
            end=run_info.updated_at,
            target_date=target_date,
        )
        metrics_df = get_metrics(
            target_date=target_date,
            company_name=run_info.company_name,
            project=run_info.project,
            run_id=run_info.run_id,
        )
        # Join
        if metrics_df.is_empty():
            _new_run_df = duration_df.with_columns(
                pl.lit(None).alias("average_gpu_utilization"),
                pl.lit(None).alias("max_gpu_utilization"),
                pl.lit(None).alias("average_gpu_memory"),
                pl.lit(None).alias("max_gpu_memory"),
            )
        else:
            _new_run_df = duration_df.join(metrics_df, on=["date"], how="left")
        new_run_df = _new_run_df.with_columns(
            pl.lit(run_info.company_name).alias("company_name"),
            pl.lit(run_info.project).alias("project"),
            pl.lit(run_info.run_id).alias("run_id"),
            pl.lit(run_info.gpu_count).alias("gpu_count"),
            pl.lit(run_info.state).alias("state"),
            pl.lit(run_info.created_at).alias("created_at"),
            pl.lit(run_info.updated_at).alias("updated_at"),
            pl.lit(logged_at).alias("logged_at"),
            pl.lit(testmode).alias("testmode"),
        )
        df_list.append(new_run_df)
    if df_list:
        new_runs_df = (
            pl.concat(df_list)
            .join(gpu_schedule_df, on=["date"], how="left")
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
    new_runs_df: pl.DataFrame, target_date: dt.date, path_to_dashboard: EasyDict, testmode: bool
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
            artifact.download("/tmp")
            old_runs_df = pl.from_pandas(
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
            # concatして重複するrunを除外
            all_runs_df = (
                pl.concat((new_runs_df.pipe(set_schema), old_runs_df.pipe(set_schema)))
                .sort(["logged_at"], descending=True)
                .unique(["date", "company_name", "project", "run_id"], keep="first")
                .sort(["run_id", "project"])
                .sort(["date"], descending=True)
                .sort(["company_name"])
            )
            assert len(all_runs_df["testmode"]) != 1, f"Testmode not matched."
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
            metadata={"version": target_date_str, "testmode": testmode},
        )
        artifact.add_file(local_path=csv_path)
        run.log_artifact(artifact)
        num_diff_records = len(all_runs_df) - len(old_runs_df)
        return {"num_diff_records": num_diff_records}
