import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import wandb
from tqdm import tqdm
from wandb_gql import gql

from utils import (
    back_to_utc,
    cast,
    log2wandb,
    remove_project_tags,
    CONFIG,
    NOW_UTC,
    QUERY,
)


# - - - - - - - - - -
# メインの処理
# - - - - - - - - - -
def get_new_runs(target_date, processed_at):
    """今日finishedになったrunとrunning状態のrunのデータを取得する"""
    df_list = []
    for company_name in tqdm(CONFIG["companies"]):
        daily_update_df = pl.DataFrame(fetch_runs(company_name)).pipe(
            process_runs, target_date=target_date, processed_at=processed_at
        )
        if not daily_update_df.is_empty():
            df_list.append(daily_update_df)
    if df_list:
        today_df = pl.concat(df_list).pipe(add_metrics).pipe(back_to_utc)
        return today_df
    else:
        return pl.DataFrame()


def update_data_src(df: pl.DataFrame, target_date) -> pl.DataFrame:
    """今日取得したrunと過去に取得したrunをconcatしてartifactsをupdateする"""
    target_date_str = target_date.strftime("%Y-%m-%d")
    with wandb.init(
        name=f"Updated_{target_date_str}",
        project="gpu-dashboard",
        job_type="update-datest",
    ) as run:
        csv_path = Path("/tmp/gpu-usage.csv")
        try:
            # 過去のrunを取得
            artifact = run.use_artifact("gpu-usage:latest")
            artifact.download("/tmp")
            old_runs_df = pl.from_pandas(
                pd.read_csv(
                    csv_path,
                    parse_dates=["created_at", "ended_at", "processed_at", "logged_at"],
                    date_format="ISO8601",
                )
            ).with_columns(
                pl.col("created_at").cast(pl.Datetime("us")),
                pl.col("ended_at").cast(pl.Datetime("us")),
                pl.col("processed_at").cast(pl.Datetime("us")),
                pl.col("logged_at").cast(pl.Datetime("us")),
            )
            if not df.is_empty():
                # concatして重複するrunを除外
                all_runs_df = (
                    pl.concat((df.pipe(cast), old_runs_df.pipe(cast)))
                    .sort("logged_at", descending=True)
                    .unique("run_path")
                    .sort(["ended_at", "logged_at"], descending=True)
                )
            else:
                all_runs_df = old_runs_df.clone()
        except:
            all_runs_df = df.clone()
        finally:
            # アーティファクト更新
            all_runs_df.write_csv(csv_path)
            artifact = wandb.Artifact(name="gpu-usage", type="dataset")
            artifact.add_file(local_path=csv_path)
            run.log_artifact(artifact)
            return all_runs_df


def remove_latest_tags() -> None:
    """プロジェクトのrunsからlatestタグを削除する"""
    remove_project_tags(
        entity=CONFIG["path_to_dashboard"]["entity"],
        project=CONFIG["path_to_dashboard"]["project"],
        delete_tags=["latest"],
        head=12,  # 最新の数件のみ（時間がかかるため）
    )
    return


def update_companies_table(df, target_date) -> None:
    if df.is_empty():
        return
    for company_name in tqdm(CONFIG["companies"]):
        run_gpu_usage = df.filter(pl.col("company_name") == company_name)
        if len(run_gpu_usage):
            daily_gpu_duration = (
                # 時間ごとで集計（日をまたぐときがあるため）
                run_gpu_usage.pipe(agg_hourly_usage)
                .with_columns(pl.col("datetime").dt.strftime("%Y-%m-%d").alias("date"))
                # 日ごとで集計
                .group_by(["company_name", "date"], maintain_order=True)
                .agg(
                    pl.col("total_gpu_hours").sum(),
                )
            )
            daily_gpu_metrics = (
                # 日ごとで集計
                run_gpu_usage.with_columns(
                    pl.col("created_at").dt.strftime("%Y-%m-%d").alias("date")
                )
                .group_by(["company_name", "date"], maintain_order=True)
                .agg(
                    [
                        pl.col("average_gpu_utilization").mean(),
                        pl.col("max_gpu_utilization").mean(),
                        pl.col("average_gpu_memory").mean(),
                        pl.col("max_gpu_memory").mean(),
                    ]
                )
            )
            # 利用時間にmetricsをマージ
            daily_gpu_usage = (
                daily_gpu_duration.join(
                    daily_gpu_metrics, on=["company_name", "date"], how="left"
                )
                .fill_null(0)
                .pipe(back_to_utc)
            )
            tables = {
                "company_run_gpu_usage": run_gpu_usage,
                "company_daily_gpu_usage": daily_gpu_usage,
            }
            target_date_str = target_date.strftime("%Y-%m-%d")
            log2wandb(
                run_name=f"Tables_{target_date_str}",
                tables=tables,
                tags=[company_name, "latest"],
            )
    return


def update_overall_table(df, target_date):
    if df.is_empty():
        return
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
    target_date_str = target_date.strftime("%Y-%m-%d")
    log2wandb(
        run_name=f"Tables_{target_date_str}",
        tables={"overall_gpu_usage": overall_usage_df},
        tags=["overall", "latest"],
    )
    return


# - - - - - - - - - -
# ヘルパー関数
# - - - - - - - - - -
def fetch_runs(company_name: str) -> list[dict[str, Any]]:
    """entityごとにGPUを使用しているrunsのデータを取得する"""
    api = wandb.Api()
    project_names = [p.name for p in api.projects(company_name)]
    gpu_info_query = gql(QUERY)

    runs_data = []
    for project_name in project_names:
        results = api.client.execute(
            gpu_info_query, {"project": project_name, "entity": company_name}
        )

        run_edges = results.get("project").get("runs").get("edges")
        runs = [e.get("node") for e in run_edges]

        for run in runs:
            # runInfoがなければスキップ
            if not run.get("runInfo"):
                continue
            # GPUなければスキップ
            if not run.get("runInfo").get("gpu"):
                continue

            # GPU使用量計算に使うデータ
            duration = run["computeSeconds"]
            runInfo = run["runInfo"]
            gpu_name = runInfo["gpu"]
            gpuCount = runInfo["gpuCount"]

            # データ追加
            runs_data.append(
                {
                    "company_name": company_name,
                    "project": project_name,
                    "run_id": run["name"],
                    "created_at": run["createdAt"],
                    "username": run["user"]["username"],
                    "gpu_name": gpu_name,
                    "gpu_count": gpuCount,
                    "duration": duration,
                    "state": run["state"],
                }
            )
    return runs_data


def process_runs(df: pl.DataFrame, target_date, processed_at) -> pl.DataFrame:
    """企業ごとのrunのテーブルを作る"""
    new_df = (
        df.with_columns(
            # 元データはUTC時間になっている
            pl.col("created_at")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S")
            .map_elements(lambda x: x + datetime.timedelta(hours=9)),
            pl.lit(processed_at).alias("processed_at"),  # 経過時間取得のために一時的に作成
            pl.lit(NOW_UTC + datetime.timedelta(hours=9)).alias("logged_at"),
        )
        .with_columns(
            # runにaccessするためにpathを作成
            pl.concat_str(
                pl.col("company_name"),
                pl.col("project"),
                pl.col("run_id"),
                separator="/",
            ).alias("run_path"),
            # 現在時刻までの経過時間
            pl.struct(["created_at", "processed_at"])
            .map_elements(
                lambda x: (x["processed_at"] - x["created_at"]).total_seconds()
            )
            .alias("elapsed_second"),
        )
        .with_columns(
            # state=runningの場合、取得時刻までの経過時間を計算する
            pl.when(pl.col("state") == "running")
            .then("elapsed_second")
            .otherwise("duration")
            .alias("duration")
        )
        .with_columns(
            (pl.col("duration") / 60 / 60).alias("duration_hour"),
            # 秒から時間に変更
        )
        .with_columns(
            # 終了時刻を取得
            (
                pl.col("created_at")
                + pl.col("duration_hour").map_elements(
                    lambda x: datetime.timedelta(hours=x)
                )
            ).alias("ended_at")
        )
        .filter(
            (pl.col("created_at").cast(pl.Date) <= target_date)
            & (pl.col("ended_at").cast(pl.Date) == target_date)
        )
        .select(
            [
                "company_name",
                "project",
                "run_id",
                "run_path",
                "username",
                "created_at",
                "ended_at",
                "processed_at",
                "logged_at",
                "state",
                "gpu_name",
                "gpu_count",
                "duration_hour",
            ]
        )
        .sort(["created_at"], descending=True)
    )
    return new_df


def add_metrics(df):
    """system metricsの統計量のカラムを追加する"""
    rows = []
    for run_path in df["run_path"]:
        sys_metrics_df = sys_metrics(run_path=run_path)
        statistics = describe_metrics(df=sys_metrics_df)
        statistics["run_path"] = run_path
        rows.append(statistics)
    new_df = df.join(pl.DataFrame(rows), on="run_path", how="left")
    return new_df


def sys_metrics(run_path: str) -> pl.DataFrame:
    """runのmetricsを取得する"""
    api = wandb.Api()
    run = api.run(path=run_path)
    df = pl.from_dataframe(run.history(stream="events"))
    return df


def describe_metrics(df: pl.DataFrame) -> dict[str, np.float16]:
    """gpu utilizationの統計量を計算する"""
    target = {"gpu_utilization": "gpu", "gpu_memory": "memory"}
    statistics = {}
    for k, v in target.items():
        try:
            array = (
                df.select(pl.col(f"^system\.gpu\..*\.{v}$"))
                .drop_nulls()
                .to_numpy()
                .ravel()
            )
            cleaned_array = array[~np.isnan(array)]
            avg_ = np.average(cleaned_array)
            max_ = np.max(cleaned_array)
        except:
            avg_, max_ = None, None
        finally:
            statistics[f"average_{k}"] = avg_
            statistics[f"max_{k}"] = max_
    return statistics


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


# 日ごとの処理
# まずは連続した日付を作る
# そこにマージしていく
