import datetime
import json
from pathlib import Path

import pandas as pd
import polars as pl
import wandb
from tqdm import tqdm

from utils import (
    back_to_utc,
    get_run_paths,
    log2wandb,
    remove_project_tags,
    CONFIG,
    NOW_UTC,
)


UPDATE_DATE = (NOW_UTC + datetime.timedelta(hours=9)).strftime("%Y-%m-%d")


def remove_latest_tags():
    remove_project_tags(
        entity=CONFIG["path_to_dashboard"]["entity"],
        project=CONFIG["path_to_dashboard"]["project"],
        delete_tags=["latest"],
    )


def read_run_table(run_path):
    api = wandb.Api()
    run = api.run(run_path)
    if not "NewRuns" in run.name:
        return
    artifacts = run.logged_artifacts()
    table_artifacts = [a for a in artifacts if a.type == "run_table"]
    artifacts_dir = table_artifacts[0].download("/tmp")
    json_path = Path(artifacts_dir) / "new_runs.table.json"
    with open(json_path, "r") as f:
        json_str = json.load(f)
    df = pl.from_dataframe(
        pd.DataFrame(json_str["data"], columns=json_str["columns"])
    ).with_columns(
        pl.from_epoch("created_at", time_unit="ms") + datetime.timedelta(hours=9),
        pl.from_epoch("ended_at", time_unit="ms") + datetime.timedelta(hours=9),
        pl.from_epoch("processed_at", time_unit="ms") + datetime.timedelta(hours=9),
        pl.from_epoch("logged_at", time_unit="ms") + datetime.timedelta(hours=9),
    )
    return df


def collect_gpu_usage():
    run_paths = get_run_paths(
        entity=CONFIG["path_to_dashboard"]["entity"],
        project=CONFIG["path_to_dashboard"]["project"],
    )
    df_list = []
    for run_path in tqdm(run_paths):  # Tablesを出力しているrunも対象になるから時間がかかる
        df = read_run_table(run_path=run_path)
        if df is not None:
            df_list.append(df)
    latest_data_df = (
        pl.concat([a for a in df_list if a is not None])
        .sort("logged_at", descending=True)
        .unique("run_path")  # 最新のものを取得
        .sort(["ended_at", "logged_at"], descending=True)
    )
    return latest_data_df


def agg_hourly_usage(df):
    """企業ごとの時間ごとの使用量を計算する"""
    company_name = df["company_name"][0]  # 企業名取得
    duration_df = df.select(["created_at", "ended_at", "gpu_count", "duration_hour"])

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
            NOW_UTC,
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


# - - - - - - - - - -
# メインの処理
# - - - - - - - - - -


def update_companies_table(latest_data_df):
    for company_name in tqdm(CONFIG["companies"]):
        run_gpu_usage = latest_data_df.filter(
            pl.col("company_name") == company_name
        ).pipe(back_to_utc)
        if len(run_gpu_usage):
            hourly_gpu_usage = run_gpu_usage.pipe(agg_hourly_usage)
            tables = {
                "company_run_gpu_usage": run_gpu_usage,
                "company_hourly_gpu_usage": hourly_gpu_usage,
                "company_hourly_gpu_usage_within_30days": hourly_gpu_usage.head(
                    30 * 24
                ),
            }
            log2wandb(
                run_name=f"Tables_{UPDATE_DATE}",
                tables=tables,
                tags=[company_name, "latest"],
            )
    return


def update_overall_table(df, logging=True):
    # 1つのDataFrameに集約してデータ整形
    overall_usage_df = (
        df.with_columns(
            pl.col("created_at").dt.strftime("%Y-%m").alias("date"),  # 月跨ぎは考慮できていない
            (pl.col("duration_hour") * pl.col("gpu_count")).alias("gpu_hours"),
        )
        .group_by(["date", "company_name"])
        .agg([pl.col("gpu_hours").sum().alias("total_gpu_hours")])
        .select(["date", "company_name", "total_gpu_hours"])
        .sort(["company_name"])
        .sort(["date"], descending=True)
    )
    if logging:
        log2wandb(
            run_name=f"Tables_{UPDATE_DATE}",
            tables={"overall_gpu_usage": overall_usage_df},
            tags=["overall", "latest"],
        )
    return overall_usage_df
