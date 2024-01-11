import json
from typing import Any, Dict, List
import wandb
from wandb_gql import gql
from tqdm import tqdm


def delete_project_tags(entity: str, project: str, delete_tags: List[str]) -> None:
    """プロジェクトのrunsからタグを削除する"""
    api = wandb.Api()
    project_path = "/".join((entity, project))
    runs = api.runs(path=project_path)
    run_ids = ["/".join((project_path, run.id)) for run in runs]
    assert run_ids, f"run_ids: {run_ids}"
    for run_id in tqdm(run_ids):
        run = api.run(path=run_id)
        old_tags = run.tags
        new_tags = [tag for tag in old_tags if tag not in delete_tags]
        run.tags = new_tags
        run.update()


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


def fetch_runs(company_name: str) -> List[Dict[str, Any]]:
    """entityからGPUを使用しているrunsのデータを取得する"""
    print(f"Getting GPU seconds by project and GPU type for entity '{company_name}'")
    api = wandb.Api()
    project_names = [p.name for p in api.projects(company_name)]
    gpu_info_query = gql(QUERY)

    runs_data = []
    for project_name in project_names:
        print(f"Scanning '{project_name}'...")

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
            # 時間が0のものはスキップ
            if run["computeSeconds"] == 0:
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
                    "created_at": run["createdAt"],
                    # "updated_at": run["updatedAt"],
                    "username": run["user"]["username"],
                    "run_name": run["name"],
                    "gpu_name": gpu_name,
                    "gpu_count": gpuCount,
                    "duration": duration,
                    # "gpu_seconds": gpuCount * duration,
                }
            )
    # デバッグのためにjsonに書き込む
    # with open(f"sample_data/{company_name}.json", "w") as f:
    # json.dump(runs_data, f, indent=4)
    return runs_data


def fetch_runs_dev(company_name: str) -> List[Dict[str, Any]]:
    """entityからGPUを使用しているrunsのデータを取得する(すでに取得してあるjsonを使用)"""
    with open(f"sample_data/{company_name}.json", "r") as f:
        runs_data = json.load(f)
    return runs_data
