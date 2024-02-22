import datetime as dt
from tqdm import tqdm
import polars as pl

from handle_runs import *


"""sample args
company_name = "turing-geniac"
target_date = dt.date(2024, 2, 21)
"""


def get_company_runs_df(company_name: str, target_date: dt.date, config):
    """企業ごとのrunのデータを取得する"""
    # get metadata of runs
    runs_info: list[RunInfo] = fetch_runs(
        company_name=company_name, target_date=target_date
    )
    df_list = []
    for run_info in tqdm(runs_info):
        ### basic table
        basic_df: pl.DataFrame = divide_duration_daily(
            start=run_info.created_at, end=run_info.updated_at
        ).with_columns(
            pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d").cast(pl.Date),
            pl.lit(run_info.company_name).alias("company_name"),
            pl.lit(run_info.project).alias("project"),
            pl.lit(run_info.run_id).alias("run_id"),
            pl.lit(run_info.gpu_count).alias("gpu_count"),
        )
        ### system_metrics
        metrics_df: pl.DataFrame = get_metrics_df(
            company_name=run_info.company_name,
            project=run_info.project,
            run_id=run_info.run_id,
        )
        if len(metrics_df) == 0:
            continue
        ### assigne gpu node
        gpu_schedule_df = get_gpu_schedule(config=config, target_date=target_date)
        ### merge and append
        tmp_df = basic_df.join(metrics_df, on=["date"], how="left").join(
            gpu_schedule_df, on=["date", "company_name"], how="left"
        )
        if tmp_df.is_empty():
            continue
        df_list.append(tmp_df)
    ### combine
    if not df_list:
        return pl.DataFrame()
    df = pl.concat(df_list)
    return df
