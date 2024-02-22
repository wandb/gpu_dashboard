import datetime as dt
from pathlib import Path

import pandas as pd
import polars as pl
from tqdm import tqdm

from handle_runs import *
from utils import cast

"""sample args
company_name = "turing-geniac"
target_date = dt.date(2024, 2, 21)
"""


def get_company_runs_df(company_name: str, target_date: dt.date, config):
    """企業ごとのrunのデータを取得する"""
    # get metadata of runs
    print(company_name)
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
        df_list.append(tmp_df.pipe(cast))
    ### combine
    if not df_list:
        return pl.DataFrame()
    new_df = pl.concat(df_list).with_columns(
        pl.lit(dt.datetime.now()).cast(pl.Datetime("us")).alias("logged_at")
    )
    return new_df


def update_artifacts(df: pl.DataFrame, target_date: dt.date, config) -> pl.DataFrame:
    """今日取得したrunと過去に取得したrunをconcatしてartifactsをupdateする"""
    target_date_str = target_date.strftime("%Y-%m-%d")
    with wandb.init(
        name=f"Update_{target_date_str}",
        project=config["path_to_dashboard"]["project"],
        job_type="update-datest",
    ) as run:
        csv_path = Path("/tmp/gpu-usage.csv")
        # 過去のrunを取得
        exist = True
        try:
            artifact = run.use_artifact("gpu-usage:latest")
        except:
            exist = False
        if exist:
            artifact.download("/tmp")
            old_runs_df = pl.from_pandas(
                pd.read_csv(
                    csv_path,
                    parse_dates=["logged_at"],
                    date_format="ISO8601",
                )
            ).with_columns(
                pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d").cast(pl.Date),
                pl.col("logged_at").cast(pl.Datetime("us")),
            )
            # concatして重複するrunを除外
            all_runs_df = (
                pl.concat((df.pipe(cast), old_runs_df.pipe(cast)))
                .sort(["logged_at"], descending=True)
                .unique(["company_name", "project", "run_id"])
                .sort(["date", "logged_at"], descending=True)
            )
        else:
            all_runs_df = df.clone()
        # アーティファクト更新
        all_runs_df.write_csv(csv_path)
        artifact = wandb.Artifact(name="gpu-usage", type="dataset")
        artifact.add_file(local_path=csv_path)
        run.log_artifact(artifact)
        return all_runs_df
