import wandb
from wandb_gql import gql

import datetime
import json
import sys

import polars as pl

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


def get_companies():
    # TODO yamlからjsonから取得
    return ["wandb-japan", "llm-jp-eval"]


def get_json(company_name):
    print(
        f"Getting GPU seconds by project and GPU type for entity '{company_name}'",
        file=sys.stderr,
    )
    api = wandb.Api()
    project_names = [p.name for p in api.projects(company_name)]  # [:2]
    gpu_info_query = gql(QUERY)

    runs_gpu_data = []
    for project_name in project_names:
        print(f"Scanning '{project_name}'...", file=sys.stderr)

        # Use internal API to make a custom GQL query
        results = api.client.execute(
            gpu_info_query, {"project": project_name, "entity": company_name}
        )

        # Destructure the result into a list of runs
        run_edges = results.get("project").get("runs").get("edges")
        runs = [e.get("node") for e in run_edges]

        # Rip through the runs and tally up duration * gpuCount for each gpu type ("gpu")
        project_gpus = {}
        for run in runs:
            duration = run["computeSeconds"]
            runInfo = run["runInfo"]
            if runInfo is None:
                continue

            gpu = runInfo["gpu"]
            gpuCount = runInfo["gpuCount"]

            if gpu is not None:
                if gpu not in project_gpus:
                    project_gpus[gpu] = 0
                if gpuCount is not None:
                    project_gpus[gpu] += gpuCount * duration
            runs_gpu_data.append(
                {
                    "created_at": run["createdAt"],
                    "updated_at": run["updatedAt"],
                    "project": project_name,
                    "username": run["user"]["username"],
                    "run_name": run["name"],
                    "gpu": gpu,
                    "gpu_count": gpuCount,
                    "duration": duration,
                    "gpu_seconds": gpuCount * duration if gpuCount is not None else 0,
                }
            )
    # json出力する場合
    # result_json = json.dumps(runs_gpu_data, indent=2)
    return runs_gpu_data


def agg_df(df):
    new_df = (
        df.filter((pl.col("gpu_seconds") != 0))
        .with_columns(pl.col("created_at").map_elements(lambda x: x[:7]).alias("date"))
        .group_by(["company_name", "date"])
        .agg([pl.col("gpu_seconds").sum().alias("total_gpu_seconds")])
        .with_columns((pl.col("total_gpu_seconds") / 60 / 60).alias("total_gpu_hours"))
        .select(["company_name", "date", "total_gpu_hours"])
        .sort(["company_name", "date"])
    )
    return new_df


def today_date():
    JST = datetime.timezone(datetime.timedelta(hours=+9))
    # 現在のJSTの時間を取得
    now_jst = datetime.datetime.now(JST)
    # 年月日までを文字列でフォーマット
    formatted_date_time = now_jst.strftime("%Y-%m-%d %H:%M")
    return formatted_date_time


if __name__ == "__main__":
    # チーム名取得
    companies = get_companies()
    # runsのデータを取得
    df_list = []
    for company_name in companies:
        runs_gpu_data = get_json(company_name=company_name)
        tmp_df = pl.DataFrame(runs_gpu_data).with_columns(
            pl.lit(company_name).alias("company_name")
        )
        df_list.append(tmp_df)
    # 1つのDataFrameに集約してデータ整形
    runs_gpu_df = pl.concat(df_list).pipe(agg_df)
    # tableを出力
    tbl = wandb.Table(data=runs_gpu_df.to_pandas())
    with wandb.init(
        entity="wandb-japan", project="gpu-dashboard", name=today_date()
    ) as run:
        wandb.log({"overall_gpu_usage": tbl})
