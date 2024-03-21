from tqdm import tqdm
import wandb

from config import CONFIG


def remove_tags() -> None:
    """プロジェクトのrunsからタグを削除する"""  # TODO リファクタ
    # get runs
    n_delete = (len(CONFIG.companies) + 2) * 3  # +2はreadとupdateの分
    api = wandb.Api()
    project_path = f"{CONFIG.dashboard.entity}/{CONFIG.dashboard.project}"
    runs = api.runs(path=project_path)
    run_paths = [f"{project_path}/{run.id}" for run in runs][:n_delete]
    # remove tags
    for run_path in tqdm(run_paths):
        run = api.run(path=run_path)
        old_tags = run.tags
        new_tags = [
            tag for tag in old_tags if tag not in CONFIG.dashboard.tag_for_latest
        ]
        run.tags = new_tags
        run.update()


