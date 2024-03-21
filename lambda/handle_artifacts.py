import pandas as pd
import polars as pl
import wandb

from config import CONFIG


def handle_artifacts(new_runs_df: pl.DataFrame) -> pl.DataFrame:
    old_runs_df = read_artifacts()
    all_runs_df = update_df(new_runs_df=new_runs_df, old_runs_df=old_runs_df)
    update_artifacts(all_runs_df=all_runs_df)
    return all_runs_df


def read_artifacts() -> pl.DataFrame:
    """artifactからデータを読み込む"""
    with wandb.init(
        entity=CONFIG.dashboard.entity,
        project=CONFIG.dashboard.project,
        name=f"Read",
    ) as run:
        try:
            # 変数
            entity = CONFIG.dataset.entity
            project = CONFIG.dataset.project
            artifact_name = CONFIG.dataset.artifact
            wandb_dir = CONFIG.wandb_dir
            # ダウンロード
            artifact_path = f"{entity}/{project}/{artifact_name}:latest"
            artifact = run.use_artifact(f"{artifact_path}")
            artifact_dir = artifact.download(wandb_dir)
            csv_path = f"{artifact_dir}/{artifact_name}.csv"
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


def update_df(new_runs_df: pl.DataFrame, old_runs_df: pl.DataFrame) -> pl.DataFrame:
    if old_runs_df.is_empty():
        all_runs_df = new_runs_df.clone()
    else:
        all_runs_df = (
            pl.concat((new_runs_df.pipe(set_schema), old_runs_df.pipe(set_schema)))
            .sort(["logged_at"], descending=True)
            .unique(["date", "company_name", "project", "run_id"], keep="first")
            .sort(["run_id", "project"])
            .sort(["date"], descending=True)
            .sort(["company_name"])
        )
    return all_runs_df


def set_schema(df: pl.DataFrame) -> pl.DataFrame:
    """Dataframeのdata型をcastする"""
    try:
        new_runs_df = df.with_columns(
            pl.col("run_id").cast(pl.Utf8),
            pl.col("assigned_gpu_node").cast(pl.Int64),
            pl.col("duration_hour").cast(pl.Float64),
            pl.col("gpu_count").cast(pl.Int64),
            pl.col("average_gpu_utilization").cast(pl.Float64),
            pl.col("average_gpu_memory").cast(pl.Float64),
            pl.col("max_gpu_utilization").cast(pl.Float64),
            pl.col("max_gpu_memory").cast(pl.Float64),
        )
        return new_runs_df
    except:
        print("!!! Failed to cast data type !!!")
        return pl.DataFrame()


def update_artifacts(all_runs_df: pl.DataFrame) -> None:
    with wandb.init(
        entity=CONFIG.dashboard.entity,
        project=CONFIG.dashboard.project,
        name=f"Update",
    ) as run:
        csv_path = f"{CONFIG.wandb_dir}/{CONFIG.dashboard.artifact}.csv"
        all_runs_df.write_csv(csv_path)
        artifact = wandb.Artifact(
            name=CONFIG.dashboard.artifact,
            type="dataset",
        )
        artifact.add_file(local_path=csv_path)
        run.log_artifact(artifact)
    return None
