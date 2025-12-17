import polars as pl
import wandb
import json
from pathlib import Path
from ..utils.config import CONFIG
from ..uploader.artifact_handler import ArtifactHandler

class DataProcessor:
    @staticmethod
    def combine_df(new_runs_df: pl.DataFrame, old_runs_df: pl.DataFrame) -> pl.DataFrame:
        if old_runs_df.is_empty():
            all_runs_df = new_runs_df.clone()
        else:
            # Handle missing user_name in old data
            if "user_name" in new_runs_df.columns and "user_name" not in old_runs_df.columns:
                print("Adding missing 'user_name' column to old runs data...")
                old_runs_df = old_runs_df.with_columns(pl.lit("unknown").alias("user_name"))

            all_runs_df = (
                pl.concat((new_runs_df.pipe(DataProcessor.set_schema), old_runs_df.pipe(DataProcessor.set_schema)))
                .sort(["logged_at"], descending=True)
                .unique(["date", "company_name", "project", "run_id"], keep="first")
                .sort(["run_id", "project"])
                .sort(["date"], descending=True)
                .sort(["company_name"])
            )
        return all_runs_df

    @staticmethod
    def set_schema(df: pl.DataFrame) -> pl.DataFrame:
        """Dataframeのdata型をcastする"""
        try:
            new_runs_df = df.with_columns(
                pl.col("run_id").cast(pl.Utf8),
                #pl.col("assigned_gpu_node").cast(pl.Int64),
                pl.col("duration_hour").cast(pl.Float64),
                pl.col("gpu_count").cast(pl.Int64),
                pl.col("average_gpu_utilization").cast(pl.Float64),
                pl.col("average_gpu_memory").cast(pl.Float64),
                pl.col("max_gpu_utilization").cast(pl.Float64),
                pl.col("max_gpu_memory").cast(pl.Float64),
            )
            # Ensure column order matches run_manager.py
            expected_columns = [
                "date", "company_name", "project", "run_id", "user_name", "tags",
                "created_at", "updated_at", "state", "duration_hour", "gpu_count",
                "average_gpu_utilization", "average_gpu_memory",
                "max_gpu_utilization", "max_gpu_memory", "host_name", "logged_at"
            ]
            # Select only existing columns (to be safe), but for concat they must match.
            # If user_name is missing (it shouldn't be for old_df after our fix in combine_df), this might fail.
            # But wait, set_schema is called AFTER the fix in combine_df? 
            # combine_df calls: new_runs_df.pipe(set_schema) AND old_runs_df.pipe(set_schema)
            # So old_runs_df is fixed BEFORE set_schema is called on it?
            # NO! Look at line 20:
            # pl.concat((new_runs_df.pipe(DataProcessor.set_schema), old_runs_df.pipe(DataProcessor.set_schema)))
            # The fix I added is in `combine_df` BEFORE this line. 
            # So yes, old_runs_df has `user_name` when it hits `set_schema`?
            # Wait, `pipe` applies the function. 
            # If I fixed `old_runs_df` in `combine_df`, then passed it to `pipe(set_schema)`, 
            # then `set_schema` receives the fixed df.
            
            return new_runs_df.select(expected_columns)
        except:
            print("!!! Failed to cast data type !!!")
            return pl.DataFrame()
