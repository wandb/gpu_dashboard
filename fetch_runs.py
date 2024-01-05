import json
from typing import Any, Dict, List

import wandb
from wandb_gql import gql

from query import QUERY


def fetch_runs(company_name: str, debug_mode=False) -> List[Dict[str, Any]]:
    """entityからGPUを使用しているrunsのデータを取得する"""
    print(f"Getting GPU seconds by project and GPU type for entity '{company_name}'")
    if debug_mode:
        with open(f"sample_data/{company_name}.json", "r") as f:
            runs_data = json.load(f)
        return runs_data
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
    return runs_data
