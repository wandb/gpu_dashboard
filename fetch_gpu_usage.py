import wandb
from wandb_gql import gql

import datetime
import sys
import yaml

import polars as pl

API = wandb.Api()

QUERY = """
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
          runInfo {
            gpuCount
            gpu
          }
        }
      }
    }
  }
}
"""


def get_runs_info(company_name):
    print(
        f"Getting GPU seconds by project and GPU type for entity '{company_name}'",
        file=sys.stderr,
    )

    project_names = [p.name for p in API.projects(company_name)]
    gpu_info_query = gql(QUERY)

    runs_gpu_data = []
    for project_name in project_names:
        print(f"Scanning '{project_name}'...", file=sys.stderr)

        # Use internal API to make a custom GQL query
        results = API.client.execute(
            gpu_info_query, {"project": project_name, "entity": company_name}
        )

        # Destructure the result into a list of runs
        # NOTE この下2行理解しておきたい
        run_edges = results.get("project").get("runs").get("edges")
        runs = [e.get("node") for e in run_edges]
        # print(runs)

        # Rip through the runs and tally up duration * gpuCount for each gpu type ("gpu")
        for run in runs:
            # runInfoがなければスキップ
            if not run["runInfo"]:
                continue

            # 素直に取得するデータ
            runInfo = run["runInfo"]
            duration = run["computeSeconds"]
            gpu_name = runInfo["gpu"]
            gpuCount = runInfo["gpuCount"]

            # データ追加
            runs_gpu_data.append(
                {
                    "project": project_name,
                    "created_at": run["createdAt"],
                    "updated_at": run["updatedAt"],
                    "run_name": run["name"],
                    "username": run["user"]["username"],
                    "gpu_name": gpu_name,
                    "gpu_count": gpuCount,
                    "duration": duration,
                    "gpu_seconds": gpuCount * duration if gpuCount else 0,
                }
            )
    return runs_gpu_data


def agg_df(df):
    """会社ごと月ごとにGPU使用量を集計する"""
    new_df = (
        df.filter((pl.col("gpu_seconds") > 0))
        .with_columns(pl.col("created_at").map_elements(lambda x: x[:7]).alias("date"))
        .group_by(["date", "company_name"])
        .agg([pl.col("gpu_seconds").sum().alias("total_gpu_seconds")])
        .with_columns((pl.col("total_gpu_seconds") / 60 / 60).alias("total_gpu_hours"))
        .select(["date", "company_name", "total_gpu_hours"])
        .sort(["company_name"])
        .sort(["date"], descending=True)
    )
    return new_df


def today_date():
    """日本の時刻を取得する"""
    # 日本の時差
    JST = datetime.timezone(datetime.timedelta(hours=+9))
    # 現在のJSTの時間を取得
    now_jst = datetime.datetime.now(JST)
    # 年月日までを文字列でフォーマット
    formatted_date_time = now_jst.strftime("%Y-%m-%d %H:%M")
    return formatted_date_time


if __name__ == "__main__":
    # 会社名取得
    with open("config.yml") as y:
        config = yaml.safe_load(y)
    # runsのデータを取得
    df_list = []
    for company_name in config["companies"]:
        runs_gpu_data = get_runs_info(company_name=company_name)
        tmp_df = pl.DataFrame(runs_gpu_data).with_columns(
            pl.lit(company_name).alias("company_name")
        )
        df_list.append(tmp_df)
    # 1つのDataFrameに集約してデータ整形
    runs_gpu_df = pl.concat(df_list).pipe(agg_df)
    print(runs_gpu_df)
    # tableを出力
    # tbl = wandb.Table(data=runs_gpu_df.to_pandas())
    # with wandb.init(
    #     entity="wandb-japan", project="gpu-dashboard", name=today_date()
    # ) as run:
    #     wandb.log({"overall_gpu_usage": tbl})
