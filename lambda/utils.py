import tqdm
import wandb

def remove_project_tags(
    entity: str, project: str, delete_tags: list[str], num: int
) -> None:
    """プロジェクトのrunsからタグを削除する"""

    def get_run_paths(entity: str, project: str) -> list[str]:
        """プロジェクト内のrun_pathを取得する"""
        api = wandb.Api()
        project_path = "/".join((entity, project))
        runs = api.runs(path=project_path)
        run_paths = ["/".join((project_path, run.id)) for run in runs]
        return run_paths

    api = wandb.Api()
    run_paths = get_run_paths(entity=entity, project=project)[:num]
    assert run_paths, f"run_paths: {run_paths}"
    for run_path in tqdm(run_paths):
        print(run_path)
        run = api.run(path=run_path)
        old_tags = run.tags
        new_tags = [tag for tag in old_tags if tag not in delete_tags]
        run.tags = new_tags
        run.update()
