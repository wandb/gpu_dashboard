import datetime
import logging
from typing import Any

import numpy as np
import polars as pl
import wandb
from tqdm import tqdm
from wandb_gql import gql

from utils import back_to_utc, log2wandb, CONFIG, PROJECT_START_DATE, NOW_UTC

# - - - - - - - - - -
# 設定
# - - - - - - - - - -
# 時刻
DATE_DIFF = -6
if DATE_DIFF == 0:
    PROCESSED_AT = NOW_UTC + datetime.timedelta(hours=9)
    TGT_DATE = NOW_UTC.date()
else:
    PROCESSED_AT = datetime.datetime.combine(
        NOW_UTC + datetime.timedelta(days=DATE_DIFF + 1), datetime.time()
    )
    TGT_DATE = (NOW_UTC + datetime.timedelta(days=DATE_DIFF, hours=9)).date()

# gqlのクエリ
QUERY = """\
query GetGpuInfoForProject($project: String!, $entity: String!) {
    project(name: $project, entityName: $entity) {
        name
        runs {
            edges {
                node {
                    name
                    user {
                        username
                    }
                    computeSeconds
                    createdAt
                    updatedAt
                    state
                    systemMetrics
                    runInfo {
                        gpuCount
                        gpu
                    }
                }
            }
        }
    }
}\
"""


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


def process_runs(df: pl.DataFrame) -> pl.DataFrame:
    """企業ごとのrunのlogのテーブルを作る"""
    new_df = (
        df.with_columns(
            # 元データはUTC時間になっている
            pl.col("created_at")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S")
            .map_elements(lambda x: x + datetime.timedelta(hours=9)),
            pl.lit(PROCESSED_AT).alias("processed_at"),  # 経過時間取得のために一時的に作成
            pl.lit(NOW_UTC + datetime.timedelta(hours=9)).alias("logged_at"),
        )
        .filter(
            # 軽量化のため過去のデータを削除
            pl.col("created_at").cast(pl.Date)
            >= PROJECT_START_DATE
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
            (pl.col("created_at").cast(pl.Date) <= TGT_DATE)
            & (pl.col("ended_at").cast(pl.Date) == TGT_DATE)
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


# - - - - - - - - - -
# メインの処理
# - - - - - - - - - -
def get_new_runs():
    """今日finishedになったrunとrunning状態のrunのデータを取得する"""
    df_list = []
    for company_name in tqdm(CONFIG["companies"]):
        logging.info(f'Retrieving "{company_name}" GPU usage ...')
        daily_update_df = pl.DataFrame(fetch_runs(company_name)).pipe(process_runs)
        df_list.append(daily_update_df)
        logging.info("Done.")
    today_df = pl.concat(df_list).pipe(add_metrics).pipe(back_to_utc)
    update_date = (TGT_DATE + datetime.timedelta(hours=9)).strftime("%Y-%m-%d")
    tables = {"new_runs": today_df}
    log2wandb(
        run_name=f"NewRuns_{update_date}",
        tables=tables,
        tags=[],
    )
    return


if __name__ == "__main__":
    get_new_runs()
