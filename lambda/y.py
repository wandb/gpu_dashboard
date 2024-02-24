import datetime as dt

from easydict import EasyDict
import polars as pl
from tqdm import tqdm

from z import fetch_runs, get_gpu_schedule, divide_duration_daily, get_metrics


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
    gpu_schedule_df = get_gpu_schedule(
        gpu_schedule=gpu_schedule, target_date=target_date
    )
    if target_date < min(gpu_schedule_df["date"]):
        print("    Not started.")
        return pl.DataFrame()
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
        new_run_df = duration_df.join(metrics_df, on=["date"], how="left").with_columns(
            pl.lit(run_info.company_name).alias("company_name"),
            pl.lit(run_info.project).alias("project"),
            pl.lit(run_info.run_id).alias("run_id"),
            pl.lit(run_info.gpu_count).alias("gpu_count"),
            pl.lit(run_info.state).alias("state"),
            pl.lit(run_info.created_at).alias("created_at"),
            pl.lit(run_info.updated_at).alias("updated_at"),
            pl.lit(logged_at).alias("logged_at"),
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
                "average_gpu_memory",
                "average_gpu_utilization",
                "max_gpu_memory",
                "max_gpu_utilization",
                "logged_at",
            )
        )
        return new_runs_df
    else:
        return pl.DataFrame()


def update_artifacts(new_runs_df: pl.DataFrame, path_to_dashboard: EasyDict) -> dict:
    return {}
