import datetime
import logging
from typing import Any, Dict, List

import numpy as np
import polars as pl
import wandb
import yaml
from wandb_gql import gql

# - - - - - - - - - -
# 設定
# - - - - - - - - - -

# 企業名の書かれたyaml
with open("config.yaml") as y:
    CONFIG = yaml.safe_load(y)
PROJECT_START_DATE = datetime.datetime(2024, 1, 1)

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

# - - - - - - - - - -
# utils
# - - - - - - - - - -


def remove_project_tags(entity: str, project: str, delete_tags: List[str]) -> None:
    """プロジェクトのrunsからタグを削除する"""
    # return
    api = wandb.Api()
    project_path = "/".join((entity, project))
    runs = api.runs(path=project_path)
    run_ids = ["/".join((project_path, run.id)) for run in runs]
    assert run_ids, f"run_ids: {run_ids}"
    for run_id in run_ids:
        run = api.run(path=run_id)
        old_tags = run.tags
        new_tags = [tag for tag in old_tags if tag not in delete_tags]
        run.tags = new_tags
        run.update()


def fetch_runs(company_name: str) -> List[Dict[str, Any]]:
    """entityごとにGPUを使用しているrunsのデータを取得する"""
    # print(f"Getting GPU seconds by project and GPU type for entity '{company_name}'")
    api = wandb.Api()
    project_names = [p.name for p in api.projects(company_name)]
    gpu_info_query = gql(QUERY)

    runs_data = []
    for project_name in project_names:
        # print(f"Scanning '{project_name}'...")
        # Use internal API to make a custom GraphQL query
        results = api.client.execute(
            gpu_info_query, {"project": project_name, "entity": company_name}
        )

        # Destructure the result into a list of runs
        run_edges = results.get("project").get("runs").get("edges")
        runs = [e.get("node") for e in run_edges]

        # Rip through the runs and tally up duration * gpuCount for each gpu type ("gpu")
        for run in runs:
            # runInfoがなければスキップ
            if not run.get("runInfo"):
                continue
            # GPUなければスキップ
            if not run.get("runInfo").get("gpu"):
                continue

            # GPU使用量計算に使うデータ
            systemMetrics = run["systemMetrics"]
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
                    # "updated_at": run["updatedAt"],
                    "username": run["user"]["username"],
                    "gpu_name": gpu_name,
                    "gpu_count": gpuCount,
                    "duration": duration,
                    # "gpu_seconds": gpuCount * duration,
                    "state": run["state"],
                }
            )
    return runs_data


def sys_metrics(entity: str, project: str, run_id: str) -> pl.DataFrame:
    """runのmetricsを取得する"""
    run_path = "/".join((entity, project, run_id))
    api = wandb.Api()
    run = api.run(path=run_path)
    sys_metrics_df = pl.from_dataframe(run.history(stream="events"))
    return sys_metrics_df


def describe_metrics(df: pl.DataFrame) -> Dict[str, np.float16]:
    """gpu utilizationの統計量を計算する"""
    array = df.select(pl.col("^system\.gpu\..*\.gpu$")).to_numpy().ravel()
    cleaned_array = array[~np.isnan(array)]
    statistics = {
        "average": np.average(cleaned_array).astype(np.float16),
        "median": np.median(cleaned_array).astype(np.float16),
        "max": np.max(cleaned_array).astype(np.float16),
    }
    return statistics


def company_runs(df: pl.DataFrame) -> pl.DataFrame:
    """企業ごとのrunのlogのテーブルを作る"""
    new_df = (
        df.with_columns(
            pl.col("created_at").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S")
        )
        # 現在時刻までの経過時間
        .with_columns(
            pl.col("created_at")
            .map_elements(lambda x: (now_utc() - x).total_seconds())
            .alias("elapsed_seconds")
        )
        .with_columns(
            pl.when(pl.col("state") == "running")
            .then("elapsed_seconds")
            .otherwise("duration")
            .alias("duration")
        )
        # 秒から時間に変更
        .with_columns(
            (pl.col("duration") / 60 / 60).alias("duration_hours"),
        )
        .select(
            [
                "company_name",
                # "project",
                "created_at",
                "run_id",
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
            now_utc(),
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


def now_utc() -> str:
    """UTCの現在時刻を取得する"""
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


def log2wandb(
    tables: Dict[str, pl.DataFrame],
    tags: List[str],
) -> None:
    """Tableをwandbに出力する"""
    entity = CONFIG["path_to_dashboard"]["entity"]
    project = CONFIG["path_to_dashboard"]["project"]
    assert wandb.Api().default_entity == entity
    config = dict(
        entity=entity,
        project=project,
        # 時差を考慮
        name=(now_utc() + datetime.timedelta(hours=9)).strftime("%Y-%m-%d %H:%M"),
        tags=tags,
    )
    with wandb.init(**config) as run:
        for tbl_name, df in tables.items():
            wandb.log({tbl_name: wandb.Table(data=df.to_pandas())})
    return None


# - - - - - - - - - -
# handler
# - - - - - - - - - -


def handler(event, context):
    # - - - - -
    # runデータ取得
    # - - - - -
    # latestタグの削除
    logging.info('removing "latest" tags...')
    remove_project_tags(
        entity=CONFIG["path_to_dashboard"]["entity"],
        project=CONFIG["path_to_dashboard"]["project"],
        delete_tags=["latest"],
    )
    logging.info("Done.")
    df_list = []
    for company_name in CONFIG["companies"]:
        logging.info(f'Processing "{company_name}" gpu usage...')
        runs_gpu_data = fetch_runs(company_name=company_name)
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
        tables = {
            "company_gpu_usage_log": company_runs_df,
            "company_hourly_gpu_usage": company_usage_df,
            "company_hourly_gpu_usage_within_30days": company_usage_df.head(30 * 24),
        }
        log2wandb(
            tables=tables,
            tags=[company_name, "latest"],
        )
        logging.info("Done.")
    # - - - - -
    # Table1
    # - - - - -
    # 1つのDataFrameに集約してデータ整形
    logging.info("Processing overal gpu usage...")
    overall_usage_df = pl.concat(df_list).pipe(overall_usage)
    log2wandb(
        tables={"overall_gpu_usage": overall_usage_df},
        tags=["overall", "latest"],
    )
    logging.info("Done.")


if __name__ == "__main__":
    handler(event=None, context=None)
