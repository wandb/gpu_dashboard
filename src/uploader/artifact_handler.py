import wandb
import json
import pandas as pd
import polars as pl
from pathlib import Path
from typing import List
from ..utils.config import CONFIG

class ArtifactHandler:
    @staticmethod
    def read_dataset() -> pl.DataFrame:
        """artifactからデータを読み込む"""
        with wandb.init(
            entity=CONFIG.dashboard.entity,
            project=CONFIG.dashboard.project,
            name="Read Dataset",
        ) as run:
            try:
                # 変数
                entity = CONFIG.dataset.entity
                project = CONFIG.dataset.project
                artifact_name = CONFIG.dataset.artifact_name
                wandb_dir = CONFIG.wandb_dir
                # ダウンロード
                artifact_path = f"{entity}/{project}/{artifact_name}:latest"
                artifact = run.use_artifact(f"{artifact_path}")
                artifact_dir = Path(artifact.download(wandb_dir))
                csv_path = artifact_dir / f"{artifact_name}.csv"
                old_runs_df = pl.from_pandas(
                    pd.read_csv(
                        csv_path,
                        parse_dates=["created_at", "updated_at", "logged_at"],
                        date_format="ISO8601",
                    )
                ).with_columns(
                    pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d").cast(pl.Date),
                    pl.col("created_at").cast(pl.Datetime("us")),
                    pl.col("updated_at").cast(pl.Datetime("us")),
                    pl.col("logged_at").cast(pl.Datetime("us")),
                )
            except Exception as e:
                print(e)
                old_runs_df = pl.DataFrame()
            finally:
                return old_runs_df
    
    @staticmethod
    def update_dataset(all_runs_df: pl.DataFrame, date_range: List[str]) -> None:
        with wandb.init(
            entity=CONFIG.dashboard.entity,
            project=CONFIG.dashboard.project,
            name=f"Update_{date_range[1]}",
        ) as run:
            filename = CONFIG.dataset.artifact_name
            csv_path = f"{CONFIG.wandb_dir}/{filename}.csv"
            all_runs_df.write_csv(csv_path)
            artifact = wandb.Artifact(
                name=filename,
                type="dataset",
            )
            artifact.add_file(local_path=csv_path)
            run.log_artifact(artifact)
    
    @staticmethod
    def read_blacklist() -> List[dict]:
        """ブラックリストのアーティファクトを読み込む"""
        with wandb.init(
            entity=CONFIG.blacklist.entity,
            project=CONFIG.blacklist.project,
            name="Read Blacklist",
        ) as run:
            try:
                artifact_path = f"{CONFIG.blacklist.entity}/{CONFIG.blacklist.project}/{CONFIG.blacklist.artifact_name}:latest"
                artifact = run.use_artifact(artifact_path)
                artifact_dir = Path(artifact.download(CONFIG.wandb_dir))
                json_path = artifact_dir / f"{CONFIG.blacklist.artifact_name}.json"
                with open(json_path, 'r') as f:
                    blacklist = json.load(f)
                return blacklist
            except Exception as e:
                print(f"Error reading blacklist: {e}")
                return []

    @staticmethod
    def upload_blacklist(blacklist: List[dict]) -> None:
        """ブラックリストをアーティファクトとしてアップロードする"""
        with wandb.init(
            entity=CONFIG.blacklist.entity,
            project=CONFIG.blacklist.project,
            name="Update Blacklist",
            job_type="update-blacklist",
        ) as run:
            filename = CONFIG.blacklist.artifact_name
            json_path = Path(CONFIG.wandb_dir) / f"{filename}.json"
            artifact = wandb.Artifact(
                name=filename,
                type="dataset",
                metadata={"record_count": len(blacklist)},
            )
            json_path.parent.mkdir(parents=True, exist_ok=True)
            with open(json_path, "w") as f:
                json.dump(blacklist, f, indent=4, sort_keys=True, ensure_ascii=False)
            artifact.add_file(local_path=str(json_path))
            run.log_artifact(artifact)