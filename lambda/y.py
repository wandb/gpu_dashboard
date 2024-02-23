import datetime as dt
from typing import Union

import polars as pl


def pipeline(
    company_name: str,
    gpu_schedule: list[dict[str, Union[str, int]]],
    target_date: dt.date,
    logged_at: dt.datetime,
) -> pl.DataFrame:
    gpu_schedule_df = pl.DataFrame(gpu_schedule).with_columns(
        pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d").cast(pl.Date),
        pl.col("assigned_gpu_node").cast(pl.Float64),
    )
    return pl.DataFrame()


def update_artifacts(new_runs_df: pl.DataFrame) -> dict:
    return {}
