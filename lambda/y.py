import datetime as dt

from easydict import EasyDict
import polars as pl


def pipeline(
    company_name: str,
    gpu_schedule: list[EasyDict],
    target_date: dt.date,
    logged_at: dt.datetime,
    testmode: bool,
) -> pl.DataFrame:
    gpu_schedule_df = pl.DataFrame(gpu_schedule).with_columns(
        pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d").cast(pl.Date),
        pl.col("assigned_gpu_node").cast(pl.Float64),
    )
    return pl.DataFrame()


def update_artifacts(new_runs_df: pl.DataFrame, path_to_dashboard: EasyDict) -> dict:
    return {}
