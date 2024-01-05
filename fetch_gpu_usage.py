import argparse
import yaml

import polars as pl
import wandb

from fetch_runs import fetch_runs
from utils import today_date


def proc_company(df):
    """企業ごとのテーブルを作る"""
    df = (
        df.with_columns(
            # datetime型に変更
            pl.col("created_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S"),
            # 秒から分に変更
            (pl.col("duration") / 60 / 60).alias("duration_hours"),
        )
        .select(
            [
                "company_name",
                # "project",
                "created_at",
                # "run_name",
                "username",
                "gpu_name",
                "gpu_count",
                # "duration",
                "duration_hours",
            ]
        )
        .sort(["created_at"], descending=True)
    )
    return df


def proc_overall(df):
    """会社ごと月ごとにGPU使用量を集計する"""
    new_df = (
        df.with_columns(
            # 年月
            pl.col("created_at").dt.strftime("%Y-%m").alias("date"),
            # gpu_hours
            (pl.col("duration_hours") * pl.col("gpu_count")).alias("gpu_hours"),
        )
        .group_by(["date", "company_name"])
        .agg([pl.col("gpu_hours").sum().alias("total_gpu_hours")])
        .select(["date", "company_name", "total_gpu_hours"])
        .sort(["company_name"])
        .sort(["date"], descending=True)
    )
    return new_df


if __name__ == "__main__":
    # 実行モード取得
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    args = parser.parse_args()
    if args.debug:
        print("Debug mode")
    else:
        print("Production mode")
    # 会社名取得
    with open("config.yml") as y:
        config = yaml.safe_load(y)
    # runsのデータを取得
    df_list = []
    for company_name in config["companies"]:
        runs_gpu_data = fetch_runs(company_name=company_name, debug_mode=args.debug)
        # - - - - -
        # Table2
        # - - - - -
        company_df = pl.DataFrame(runs_gpu_data).pipe(proc_company)
        df_list.append(company_df)
        if args.debug:
            print(company_df)
    # - - - - -
    # Table1
    # - - - - -
    # 1つのDataFrameに集約してデータ整形
    runs_gpu_df = pl.concat(df_list).pipe(proc_overall)
    if args.debug:  # デバッグモード
        print(runs_gpu_df)
    else:
        tbl = wandb.Table(data=runs_gpu_df.to_pandas())
        with wandb.init(
            entity="wandb-japan", project="gpu-dashboard", name=today_date()
        ) as run:
            wandb.log({"overall_gpu_usage": tbl})
