from dataclasses import dataclass
import json

from tqdm import tqdm
import wandb

from config import CONFIG
from fetch_runs import Project, plant_trees, query_runs


@dataclass
class BlacklistRow:
    run_path: str
    tags: list[str]


def update_blacklist() -> None:
    blacklist = create_blacklist()
    upload_blacklist(blacklist=blacklist)
    return None


def create_blacklist() -> list[BlacklistRow]:
    trees = plant_trees()

    print("Get projects for each team ...")
    for tree in trees:
        projects = [Project(project=p.name) for p in wandb.Api().projects(tree.team)]
        tree.projects = projects

    print("Get runs for each project ...")
    for tree in tqdm(trees):
        print("Team:", tree.team)
        for project in tqdm(tree.projects):
            runs = query_runs(
                team=tree.team,
                project=project.project,
                target_date=None,
                make_blacklist=True,
            )
            project.runs = runs

    print("Update blacklist of runs ...")
    blacklist = []
    for tree in trees:
        for project in tree.projects:
            for run in project.runs:
                if CONFIG.ignore_tag in [t.lower() for t in run.tags]:
                    blacklist.append(BlacklistRow(run_path=run.run_path, tags=run.tags))

    return blacklist


def upload_blacklist(blacklist: list[BlacklistRow]) -> None:
    with wandb.init(
        entity=CONFIG.blacklist.entity,
        project=CONFIG.blacklist.project,
        name="Update Blacklist",
        job_type="update-blacklist",
        tags=[],
    ) as run:
        filename = CONFIG.blacklist.artifact_name
        json_path = f"blacklist/{filename}.json"
        artifact = wandb.Artifact(
            name=filename,
            type="dataset",
            metadata={"record_count": len(blacklist)},
        )
        with open(json_path, "w") as f:
            json.dump(
                [b.__dict__ for b in blacklist],
                f,
                indent=4,
                sort_keys=True,
                ensure_ascii=False,
            )
        artifact.add_file(local_path=json_path)
        run.log_artifact(artifact)

    return None

if __name__ == "__main__":
    update_blacklist()
