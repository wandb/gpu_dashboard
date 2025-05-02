import wandb
import pandas as pd
import polars as pl
from pathlib import Path
from typing import List
from src.utils.config import CONFIG

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
