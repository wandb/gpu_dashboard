import argparse
import datetime
import yaml
from typing import List

import polars as pl
import wandb

from utils import fetch_runs, fetch_runs_dev, delete_project_tags

with open("config.yml") as y:  # 企業名の書かれたyaml
    CONFIG = yaml.safe_load(y)


def company_runs(df):
    """企業ごとのrunのlogのテーブルを作る"""
    new_df = (
        df.with_columns(
            # datetime型に変更
            pl.col("created_at")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S")
            .map_elements(lambda x: x + datetime.timedelta(hours=-9)),  # 時差を考慮
            # 秒から時間に変更
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
    return new_df


def company_usage(df):
    """企業ごとの時間ごとの使用量を計算する"""
    company_name = df["company_name"][0]  # 企業名取得
    duration_df = df.with_columns(
        # 終了時刻を取得
        (
            pl.col("created_at")
            + pl.col("duration_hours").map_elements(
                lambda x: datetime.timedelta(hours=x)
            )
        ).alias("ended_at")
    ).select(["created_at", "ended_at", "gpu_count", "duration_hours"])

    # 行ごとにdictにする
    duration_list = duration_df.to_pandas().to_dict(orient="records")

    df_list = []
    for run in duration_list:
        datetime_srs = (
            # 分刻みのデータに拡張
            pl.datetime_range(
                start=run["created_at"], end=run["ended_at"], interval="1m", eager=True
            )
            .dt.strftime("%Y-%m-%d %H:00")  # minutesを0にする
            .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M")  # datetime型に戻す
        )
        tmp_df = pl.DataFrame(datetime_srs.alias("datetime")).with_columns(
            pl.lit(run["gpu_count"]).alias("gpu_count")  # gpu数のカラムを追加
        )
        df_list.append(tmp_df)

    usage_per_hours = (
        pl.concat(df_list)  # runを集約
        .group_by(["datetime"], maintain_order=True)
        .agg((pl.col("gpu_count").sum() / 60).alias("total_gpu_hours"))  # 時間の単位を変更
    )

    # 連続した日付を持つDataFrame
    expanded_datetime = pl.DataFrame(
        pl.datetime_range(
            usage_per_hours["datetime"].min(),
            usage_per_hours["datetime"].max(),
            interval="1h",
            eager=True,
        ).alias("datetime")
    )

    # マージしてデータがない部分は0で埋める
    company_usage = (
        expanded_datetime.join(usage_per_hours, on="datetime", how="left")
        .fill_null(0)
        .with_columns(pl.lit(company_name).alias("company_name"))  # 企業名のカラムを追加
        .select(["company_name", "datetime", "total_gpu_hours"])
        .sort("datetime", descending=True)
    )

    return company_usage


def overall_usage(df):
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


def today_date() -> str:
    """日本の時刻を取得する"""
    # 日本の時差
    JST = datetime.timezone(datetime.timedelta(hours=+9))
    # 現在のJSTの時間を取得
    now_jst = datetime.datetime.now(JST)
    # 年月日までを文字列でフォーマット
    formatted_date_time = now_jst.strftime("%Y-%m-%d %H:%M")
    return formatted_date_time


def log2wandb(
    df,
    tbl_name: str,
    tags: List[str],
) -> None:
    assert wandb.Api().default_entity == "wandb-japan"
    "DataFrameをWandBに出力する"
    tbl = wandb.Table(data=df.to_pandas())
    config = dict(
        entity="wandb-japan", project="gpu-dashboard", name=today_date(), tags=tags
    )
    with wandb.init(**config) as run:
        wandb.log({tbl_name: tbl})
    return None


if __name__ == "__main__":
    # - - - - -
    # 実行モード取得
    # - - - - -
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    args = parser.parse_args()
    if args.debug:
        print("Debug mode")
    else:
        print("Production mode")
    # - - - - -
    # runデータ取得
    # - - - - -
    # latestタグの削除
    if not args.debug:
        delete_project_tags(
            entity=CONFIG["path_to_dashboard"]["entity"],
            project=CONFIG["path_to_dashboard"]["project"],
            delete_tags=["latest"],
        )
    df_list = []
    for company_name in CONFIG["companies"]:
        # debugモードのときはjsonをread
        runs_gpu_data = (
            fetch_runs(company_name=company_name)
            if not args.debug
            else fetch_runs_dev(company_name=company_name)
        )
        # - - - - -
        # Table2
        # - - - - -
        company_runs_df = pl.DataFrame(runs_gpu_data).pipe(company_runs)
        # - - - - -
        # Table3
        # - - - - -
        company_usage_df = company_runs_df.pipe(company_usage)
        # Table1のためにリストに追加
        df_list.append(company_runs_df)
        # 出力
        if args.debug:
            print(company_runs_df)
            print(company_usage_df)
            print(company_usage_df.head(30 * 24))
        else:
            log2wandb(
                df=company_runs_df,
                tbl_name="company_gpu_usage_log",
                tags=[company_name, "latest"],
            )
            log2wandb(
                df=company_usage_df,
                tbl_name="company_hourly_gpu_usage",
                tags=[company_name, "latest"],
            )
            log2wandb(
                df=company_usage_df.head(30 * 24),
                tbl_name="company_hourly_gpu_usage_within_30days",
                tags=[company_name, "latest"],
            )
    # - - - - -
    # Table1
    # - - - - -
    # 1つのDataFrameに集約してデータ整形
    overall_usage_df = pl.concat(df_list).pipe(overall_usage)
    if args.debug:  # デバッグモード
        print(overall_usage_df)
        print(len(overall_usage_df))
    else:
        log2wandb(
            df=overall_usage_df,
            tbl_name="overall_gpu_usage",
            tags=["overall", "latest"],
        )
