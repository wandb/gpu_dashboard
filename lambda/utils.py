import datetime

import polars as pl
import wandb
import yaml
from tqdm import tqdm

# 企業名の書かれたyaml
with open("config.yaml") as y:
    CONFIG = yaml.safe_load(y)

# 日付
PROJECT_START_DATE = datetime.date(2024, 1, 1)
NOW_UTC = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


def back_to_utc(df):
    """UI上で日本時間になるようにするためUTC時間に戻す"""  # TODO ダサいからリファクタ
    datetime_cols = ["created_at", "ended_at", "logged_at", "processed_at"]
    new_df = df.clone()
    for col in datetime_cols:
        try:
            new_df = new_df.with_columns(
                pl.col(col).map_elements(lambda x: x + datetime.timedelta(hours=-9)),
            )
        except:
            pass
    return new_df


def get_run_paths(entity, project):
    """プロジェクト内のrun_pathを取得する"""
    api = wandb.Api()
    project_path = "/".join((entity, project))
    runs = api.runs(path=project_path)
    run_paths = ["/".join((project_path, run.id)) for run in runs]
    return run_paths


def log2wandb(
    run_name: str,
    tables: dict[str, pl.DataFrame],
    tags: list[str],
) -> None:
    """Tableをwandbに出力する"""
    entity = CONFIG["path_to_dashboard"]["entity"]
    project = CONFIG["path_to_dashboard"]["project"]
    assert wandb.Api().default_entity == entity
    config = dict(
        entity=entity,
        project=project,
        # 時差を考慮
        name=run_name,
        tags=tags,
    )
    with wandb.init(**config) as run:
        for tbl_name, df in tables.items():
            wandb.log({tbl_name: wandb.Table(data=df.to_pandas())})
    return None


def remove_project_tags(entity: str, project: str, delete_tags: list[str]) -> None:
    """プロジェクトのrunsからタグを削除する"""
    api = wandb.Api()
    run_paths = get_run_paths(entity=entity, project=project)[:8]
    assert run_paths, f"run_paths: {run_paths}"
    for run_path in tqdm(run_paths):
        print(run_path)
        run = api.run(path=run_path)
        old_tags = run.tags
        new_tags = [tag for tag in old_tags if tag not in delete_tags]
        run.tags = new_tags
        run.update()
