import datetime as dt
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

GPU_PER_NODE = 8
HOUR_PER_DAY = 24
RATIO_TO_PERCENT = 100
MARK = "test"


# - - - - - - - - - -
# ヘルパー関数
# - - - - - - - - - -
def get_gpu_schedule(config: dict[str, Any], target_date: dt.date) -> pl.DataFrame:
    """日次のGPU割り当て数を取得する"""
    df_list = []
    for company in config["companies"]:
        start_date: dt.date = get_start_date(company_cfg=company)
        date_df = pl.DataFrame(
            pl.datetime_range(
                start=start_date, end=target_date, interval="1d", eager=True
            )
            .dt.strftime("%Y-%m-%d")
            .alias("date")
            .alias("date")
        )
        gpu_df = date_df.join(
            pl.DataFrame(company["schedule"]), on="date", how="left"
        ).with_columns(
            pl.col("assigned_gpu_node").forward_fill(),
            pl.lit(company["company_name"]).alias("company_name"),
        )
        df_list.append(gpu_df)
    new_df = pl.concat(df_list)
    return new_df


def get_start_date(company_cfg: dict[str, Any]) -> dt.date:
    "企業のGPU割り当て開始日を取得する"
    start_date: dt.date = min(
        dt.datetime.strptime(schedule["date"], "%Y-%m-%d").date()
        for schedule in company_cfg["schedule"]
    )
    return start_date


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
            runInfo = run["runInfo"]
            gpu_name = runInfo["gpu"]
            gpuCount = runInfo["gpuCount"]
            createdAt = run["createdAt"]
            updatedAt = run["updatedAt"]
            duration = (
                dt.datetime.fromisoformat(updatedAt).timestamp()
                - dt.datetime.fromisoformat(createdAt).timestamp()
            )  # すべて秒数にしてから引き算
            # duration = run["computeSeconds"]

            # データ追加
            runs_data.append(
                {
                    "company_name": company_name,
                    "project": project_name,
                    "run_id": run["name"],
                    "created_at": createdAt,
                    "updated_at": updatedAt,
                    "username": run["user"]["username"],
                    "gpu_name": gpu_name,
                    "gpu_count": gpuCount,
                    "duration": duration,
                    "state": run["state"],
                }
            )
    return runs_data


def process_runs(
    df: pl.DataFrame,
    target_date: dt.date,
    start_date: dt.date,
) -> pl.DataFrame:
    """企業ごとのrunのテーブルを作る"""
    if df.is_empty():
        return pl.DataFrame()
    new_df = (
        df.with_columns(
            # 元データはUTC時間になっている
            pl.col("created_at")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S")
            .map_elements(lambda x: x + dt.timedelta(hours=9)),
            pl.lit(NOW_UTC + dt.timedelta(hours=9)).alias("logged_at"),
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
        )
        .with_columns(
            pl.col("duration").truediv(60**2).alias("duration_hour"),
            # 秒から時間に変更
        )
        .with_columns(
            # 終了時刻を取得
            pl.col("created_at")
            .add(pl.col("duration_hour").map_elements(lambda x: dt.timedelta(hours=x)))
            .alias("ended_at")
        )
        .filter(
            # GPU使用開始後のrun
            (pl.col("created_at") >= dt.datetime.combine(start_date, dt.time()))
            # ターゲット日より前に作られたrun
            & (pl.col("created_at").cast(pl.Date) <= target_date)
            # 今日終了したrun
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


def add_metrics(df: pl.DataFrame) -> pl.DataFrame:
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


def agg_hourly_usage(df: pl.DataFrame) -> pl.DataFrame:
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
        .agg(
            pl.col("gpu_count").sum().truediv(60).alias("total_gpu_hours")
        )  # 時間の単位を変更
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
        .with_columns(
            pl.lit(company_name).alias("company_name")
        )  # 企業名のカラムを追加
        .select(["company_name", "datetime", "total_gpu_hours"])
        .sort(["datetime"], descending=True)
    )
    return company_usage


# - - - - - - - - - -
# メインの処理
# - - - - - - - - - -
def get_new_runs(
    target_date: dt.date,
) -> pl.DataFrame:
    """今日finishedになったrunとrunning状態のrunのデータを取得する"""
    df_list = []
    for company in tqdm(CONFIG["companies"]):
        start_date: dt.date = get_start_date(company_cfg=company)
        daily_update_df = pl.DataFrame(fetch_runs(company["company_name"])).pipe(
            process_runs,
            target_date=target_date,
            start_date=start_date,
        )
        if not daily_update_df.is_empty():
            df_list.append(daily_update_df)
    if df_list:
        today_df = pl.concat(df_list).pipe(add_metrics).pipe(back_to_utc)
        return today_df
    else:
        return pl.DataFrame()


def update_artifacts(df: pl.DataFrame, target_date: dt.date) -> pl.DataFrame:
    """今日取得したrunと過去に取得したrunをconcatしてartifactsをupdateする"""
    target_date_str = target_date.strftime("%Y-%m-%d")
    with wandb.init(
        name=f"Update_{target_date_str}",
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
                    parse_dates=["created_at", "ended_at", "logged_at"],
                    date_format="ISO8601",
                )
            ).with_columns(
                pl.col("created_at").cast(pl.Datetime("us")),
                pl.col("ended_at").cast(pl.Datetime("us")),
                pl.col("logged_at").cast(pl.Datetime("us")),
            )
            if not df.is_empty():
                # concatして重複するrunを除外
                all_runs_df = (
                    pl.concat((df.pipe(cast), old_runs_df.pipe(cast)))
                    .sort(["logged_at"], descending=True)
                    .unique("run_path")
                    .sort(["created_at", "logged_at"], descending=True)
                )
            else:
                all_runs_df = old_runs_df.clone()
        except:
            all_runs_df = df.clone()
        finally:
            # アーティファクト更新
            # all_runs_df.write_csv(csv_path)
            # artifact = wandb.Artifact(name="gpu-usage", type="dataset")
            # artifact.add_file(local_path=csv_path)
            # run.log_artifact(artifact)
            return all_runs_df


def remove_latest_tags() -> None:
    """プロジェクトのrunsからlatestタグを削除する"""
    DAYS_TO_RESET = 2
    remove_project_tags(
        entity=CONFIG["path_to_dashboard"]["entity"],
        project=CONFIG["path_to_dashboard"]["project"],
        delete_tags=[MARK],
        head=(2 + len(CONFIG["companies"]))
        * DAYS_TO_RESET,  # 最新の数件のみ（時間がかかるため）
    )
    return


def update_companies_table(df: pl.DataFrame, target_date: dt.date) -> None:
    """企業のテーブルを更新する"""
    # TODO 該当日に利用実績がなかったときの対応
    if df.is_empty():
        return pl.DataFrame()
    df_list = []
    for company in tqdm(CONFIG["companies"]):
        run_gpu_usage = df.filter(pl.col("company_name") == company["company_name"])
        if run_gpu_usage.is_empty():
            continue
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
                pl.col("average_gpu_utilization").mean(),
                pl.col("max_gpu_utilization").mean(),
                pl.col("average_gpu_memory").mean(),
                pl.col("max_gpu_memory").mean(),
            )
        )
        # 日次のGPU割り当て数を取得
        gpu_schedule_df = get_gpu_schedule(config=CONFIG, target_date=target_date)
        start_date: dt.date = get_start_date(company_cfg=company)
        # 利用時間にmetricsをマージ
        daily_gpu_usage = (
            daily_gpu_duration.join(
                daily_gpu_metrics, on=["company_name", "date"], how="left"
            )
            .join(
                gpu_schedule_df, on=["company_name", "date"], how="left"
            )  # 割り当てGPU数をマージ
            .with_columns(
                pl.col("total_gpu_hours")
                .truediv(
                    pl.col("assigned_gpu_node").mul(GPU_PER_NODE).mul(HOUR_PER_DAY)
                )
                .mul(RATIO_TO_PERCENT)
                .alias("utilization_rate")
            )
            .filter(
                pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d")
                >= (dt.datetime.combine(start_date, dt.time()))
            )
            .pipe(back_to_utc)
        )
        target_date_str = target_date.strftime("%Y-%m-%d")
        log2wandb(
            run_name=f"Tables_{target_date_str}",
            tables={
                "company_daily_gpu_usage": daily_gpu_usage.fill_null(0),
                "company_daily_gpu_usage_within_30days": daily_gpu_usage.fill_null(
                    0
                ).head(30),
            },
            tags=[company["company_name"], MARK],
        )
        df_list.append(daily_gpu_usage)
    # 各企業のdfをconcat
    new_df = pl.concat(df_list)
    return new_df


def update_overall_table(df: pl.DataFrame, target_date: dt.date) -> None:
    if df.is_empty():
        return
    # GPU割り当て数と利用可能な時間を取得
    gpu_schedule_df = get_gpu_schedule(config=CONFIG, target_date=target_date)
    overall_gpu_df = gpu_schedule_df.group_by(["company_name"]).agg(
        pl.col("assigned_gpu_node")
        .sum()
        .mul(GPU_PER_NODE)
        .mul(HOUR_PER_DAY)
        .alias("assigned_gpu_hours")
    )
    # 利用実績を集計
    grouped_df = df.group_by("company_name").agg(
        pl.col("total_gpu_hours").sum(),
        pl.col("average_gpu_utilization").mean(),
        pl.col("max_gpu_utilization").mean(),
        pl.col("average_gpu_memory").mean(),
        pl.col("max_gpu_memory").mean(),
    )
    # joinしてカラム作成
    overall_usage_df = overall_gpu_df.join(
        grouped_df, on=["company_name"], how="left"
    ).with_columns(
        pl.col("total_gpu_hours")
        .truediv(pl.col("assigned_gpu_hours"))
        .mul(RATIO_TO_PERCENT)
        .alias("utilization_rate")
    )
    mothly_usage_df = (
        gpu_schedule_df.join(df, on=["company_name", "date"], how="left")
        .with_columns(
            pl.col("date")
            .str.strptime(pl.Datetime, "%Y-%m-%d")
            .dt.strftime("%Y-%m")
            .alias("year_month")
        )
        .group_by(["company_name", "year_month"])
        .agg(
            pl.col("total_gpu_hours").sum(),
            pl.col("average_gpu_utilization").mean(),
            pl.col("max_gpu_utilization").mean(),
            pl.col("average_gpu_memory").mean(),
            pl.col("max_gpu_memory").mean(),
            pl.col("assigned_gpu_node")
            .sum()
            .mul(GPU_PER_NODE)
            .mul(HOUR_PER_DAY)
            .alias("assigned_gpu_hours"),
        )
        .with_columns(
            pl.col("total_gpu_hours")
            .truediv(pl.col("assigned_gpu_hours"))
            .mul(RATIO_TO_PERCENT)
            .alias("utilization_rate")
        )
        .sort(["company_name"])
        .sort(["year_month"], descending=True)
    )
    target_date_str = target_date.strftime("%Y-%m-%d")
    log2wandb(
        run_name=f"Tables_{target_date_str}",
        tables={
            "overall_gpu_usage": overall_usage_df,
            "monthly_gpu_usage": mothly_usage_df,
        },
        tags=["overall", MARK],
    )
    return
