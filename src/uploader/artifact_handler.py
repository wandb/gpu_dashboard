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
                
                # Get version from environment variable or default to latest
                import os
                version = os.environ.get("WANDB_ARTIFACT_VERSION", "latest")
                print(f"Using artifact version: {version}")
                
                # ダウンロード
                artifact_path = f"{entity}/{project}/{artifact_name}:{version}"
                artifact = run.use_artifact(f"{artifact_path}")
                
                old_runs_df = pl.DataFrame()
                
                # Try to get as Table first (Scalable & Cleaner)
                try:
                    table = artifact.get(artifact_name)
                    if table:
                        print("Loading data from WandB Table (Scalable)...")
                        pdf = table.get_dataframe()
                        # Ensure datetime columns are datetime objects
                        for col in ["created_at", "updated_at", "logged_at"]:
                            if col in pdf.columns:
                                pdf[col] = pd.to_datetime(pdf[col])
                        old_runs_df = pl.from_pandas(pdf)
                    else:
                        print("Table object not found in artifact.")
                except Exception as e:
                    print(f"Failed to load table: {e}")

                # Fallback to CSV download if Table failed or was empty
                if old_runs_df.is_empty():
                    print("Falling back to CSV download...")
                    artifact_dir = Path(artifact.download(wandb_dir))
                    csv_path = artifact_dir / f"{artifact_name}.csv"
                    if csv_path.exists():
                        old_runs_df = pl.from_pandas(
                            pd.read_csv(
                                csv_path,
                                parse_dates=["created_at", "updated_at", "logged_at"],
                                date_format="ISO8601",
                            )
                        )
                
                # Type casting - handle different source types
                if not old_runs_df.is_empty():
                    # Handle 'date' column - might be String, Date, or Datetime
                    date_dtype = old_runs_df.schema.get("date")
                    if date_dtype == pl.Utf8:
                        # String -> parse to Date
                        old_runs_df = old_runs_df.with_columns(
                            pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
                        )
                    elif date_dtype != pl.Date:
                        # Some kind of Datetime (e.g., Datetime(ns, UTC)) -> convert to Date
                        old_runs_df = old_runs_df.with_columns(
                            pl.col("date").cast(pl.Datetime).dt.date().alias("date")
                        )
                    
                    # Handle timestamp columns - strip timezone if present
                    for ts_col in ["created_at", "updated_at", "logged_at"]:
                        if ts_col in old_runs_df.columns:
                            col_dtype = old_runs_df.schema.get(ts_col)
                            if col_dtype is not None and hasattr(col_dtype, 'time_zone') and col_dtype.time_zone is not None:
                                # Has timezone, strip it
                                old_runs_df = old_runs_df.with_columns(
                                    pl.col(ts_col).dt.replace_time_zone(None).cast(pl.Datetime("us"))
                                )
                            else:
                                old_runs_df = old_runs_df.with_columns(
                                    pl.col(ts_col).cast(pl.Datetime("us"))
                                )
                    
            except Exception as e:
                print(f"Error reading dataset: {e}")
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
            artifact = wandb.Artifact(
                name=filename,
                type="dataset",
            )
            
            # 1. Add as WandB Table (Primary, Scalable)
            # Convert Polars to Pandas for WandB
            print("Adding data as WandB Table...")
            pdf = all_runs_df.to_pandas()
            # Convert timestamp columns to proper format if needed, but WandB handles datetime64
            table = wandb.Table(dataframe=pdf)
            artifact.add(table, filename)
            
            # 2. Add as CSV (Legacy Support)
            print("Adding data as CSV (Legacy)...")
            csv_path = f"{CONFIG.wandb_dir}/{filename}.csv"
            all_runs_df.write_csv(csv_path)
            artifact.add_file(local_path=csv_path)
            
            run.log_artifact(artifact)
