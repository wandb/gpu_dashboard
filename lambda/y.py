import datetime as dt
from pathlib import Path

from easydict import EasyDict
import pandas as pd
import polars as pl
from tqdm import tqdm
import wandb

from z import (
    fetch_runs,
    get_gpu_schedule,
    divide_duration_daily,
    get_metrics,
    read_table_csv,
    daily_summarize,
    set_schema,
    get_whole_gpu_schedule,
    monthly_summarize,
    overall_summarize,
)


def pipeline(
    company_name: str,
    gpu_schedule: list[EasyDict],
    target_date: dt.date,
    logged_at: dt.datetime,
    ignore_project: str,
    ignore_tag: str,
    testmode: bool,
) -> pl.DataFrame:
    ### GPUスケジュール
    print()
    print(f"Processing {company_name} ...")
    if target_date < min(pl.DataFrame(gpu_schedule)["date"].cast(pl.Date)):
        print("    Not started.")
        return pl.DataFrame()
    gpu_schedule_df: pl.Dataframe = get_gpu_schedule(
        gpu_schedule=gpu_schedule, target_date=target_date
    )
    ### Run取得
    runs_info = fetch_runs(
        company_name=company_name,
        target_date=target_date,
        ignore_project=ignore_project,
        ignore_tag=ignore_tag,
        testmode=testmode,
    )
    if not runs_info:
        print("  No runs found.")
        return pl.DataFrame()
    ### Process runs
    df_list = []
    for run_info in tqdm(runs_info):
        # Process each runs
        if (testmode) & (len(df_list) == 2):
            continue
        duration_df: pl.Dataframe = divide_duration_daily(
            start=run_info.created_at,
            end=run_info.updated_at,
            target_date=target_date,
        ).with_columns(
            pl.lit(run_info.company_name).cast(pl.String).alias("company_name"),
            pl.lit(run_info.project).cast(pl.String).alias("project"),
            pl.lit(run_info.run_id).cast(pl.String).alias("run_id"),
            pl.lit(run_info.created_at).cast(pl.Datetime).alias("created_at"),
            pl.lit(run_info.updated_at).cast(pl.Datetime).alias("updated_at"),
            pl.lit(run_info.state).cast(pl.String).alias("state"),
            pl.lit(run_info.gpu_count).cast(pl.Float64).alias("gpu_count"),
        )
        metrics_df: pl.Dataframe = get_metrics(
            target_date=target_date,
            company_name=run_info.company_name,
            project=run_info.project,
            run_id=run_info.run_id,
        )
        # Join
        new_run_df: pl.Dataframe
        if metrics_df.is_empty():
            new_run_df = duration_df.with_columns(
                pl.lit(None).cast(pl.Float64).alias("average_gpu_utilization"),
                pl.lit(None).cast(pl.Float64).alias("max_gpu_utilization"),
                pl.lit(None).cast(pl.Float64).alias("average_gpu_memory"),
                pl.lit(None).cast(pl.Float64).alias("max_gpu_memory"),
            )
        else:
            new_run_df = duration_df.join(metrics_df, on=["date"], how="left")
        df_list.append(new_run_df)
    if df_list:
        new_runs_df: pl.Dataframe = (
            pl.concat(df_list)
            .join(gpu_schedule_df, on=["date"], how="left")
            .with_columns(
                pl.lit(logged_at).cast(pl.Datetime).alias("logged_at"),
                pl.lit(testmode).cast(bool).alias("testmode"),
            )
            .select(
                "date",
                "company_name",
                "project",
                "run_id",
                "assigned_gpu_node",
                "created_at",
                "updated_at",
                "state",
                "duration_hour",
                "gpu_count",
                "average_gpu_utilization",
                "average_gpu_memory",
                "max_gpu_utilization",
                "max_gpu_memory",
                "logged_at",
                "testmode",
            )
        )
        return new_runs_df
    else:
        return pl.DataFrame()


def update_artifacts(
    new_runs_df: pl.DataFrame,
    target_date: dt.date,
    wandb_dir: str,
    path_to_dashboard: EasyDict,
    elapsed_time: str,
    testmode: bool,
) -> dict:
    """今日取得したrunと過去に取得したrunをconcatしてartifactsをupdateする"""
    target_date_str = target_date.strftime("%Y-%m-%d")
    with wandb.init(
        entity=path_to_dashboard.entity,
        project=path_to_dashboard.project,
        name=f"Update_{target_date_str}",
        job_type="update-datest",
    ) as run:
        csv_path = Path(f"/tmp/{path_to_dashboard.artifact_name}.csv")
        # 過去のrunの存在を確認
        exist = True
        try:
            artifact = run.use_artifact(f"{path_to_dashboard.artifact_name}:latest")
        except:
            exist = False
        if exist:
            # 過去のrunを取得
            old_runs_df = read_table_csv(
                run=run,
                wandb_dir=wandb_dir,
                artifact_name=path_to_dashboard.artifact_name,
            )
            # concatして重複するrunを除外
            all_runs_df = (
                pl.concat((new_runs_df.pipe(set_schema), old_runs_df.pipe(set_schema)))
                .sort(["logged_at"], descending=True)
                .unique(["date", "company_name", "project", "run_id"], keep="first")
                .sort(["run_id", "project"])
                .sort(["date"], descending=True)
                .sort(["company_name"])
            )
            assert len(all_runs_df) >= len(
                old_runs_df
            ), f"!!! Data length error !!! all: {len(all_runs_df)}, old: {len(old_runs_df)}"
        else:
            if new_runs_df.is_empty():
                # Artifactが存在しなくて、新しいrunもない
                return {"message": "No runs found"}
            old_runs_df = pl.DataFrame()
            all_runs_df = new_runs_df.clone()
        # アーティファクト更新
        all_runs_df.write_csv(csv_path)
        artifact = wandb.Artifact(
            name=path_to_dashboard.artifact_name,
            type="dataset",
            metadata={
                "target_date": target_date_str,
                "elapsed_time": elapsed_time,
                "testmode": testmode,
            },
        )
        artifact.add_file(local_path=csv_path)
        run.log_artifact(artifact)
        new_records = len(all_runs_df) - len(old_runs_df)
        return new_records


def update_tables(
    wandb_dir: str,
    companies_config: list[EasyDict],
    path_to_dashboard: EasyDict,
    target_date: dt.date,
    tag_for_latest: str,
) -> list[pl.DataFrame]:
    ### Fetch csv
    with wandb.init(
        entity=path_to_dashboard.entity,
        project=path_to_dashboard.project,
        name=f"Read_{target_date.strftime('%Y-%m-%d')}",
        job_type="read-table",
    ) as run:
        all_runs_df = read_table_csv(
            run=run, wandb_dir=wandb_dir, artifact_name=path_to_dashboard.artifact_name
        )
    ### Source of tables
    daily_df = get_whole_gpu_schedule(
        companies_config=companies_config, target_date=target_date
    ).filter(pl.col("date") <= target_date)
    if daily_df.is_empty():
        return {"message": "Not started yet."}
    ### Update tables
    target_date_str = target_date.strftime("%Y-%m-%d")
    # Monthly and overall tables
    with wandb.init(
        entity=path_to_dashboard.entity,
        project=path_to_dashboard.project,
        name=f"Tables_{target_date_str}",
        job_type="update-table",
        tags=["overall", tag_for_latest],
    ) as run:
        ### Overall tables
        gpu_schedule_df = (
            get_whole_gpu_schedule(
                companies_config=companies_config, target_date=target_date
            )
            .with_columns(
                pl.col("date").min().alias("start_date"),
            )
            .group_by("company_name")
            .agg(
                pl.col("assigned_gpu_node").sum(),
                pl.col("date").count().alias("days"),
            )
            .with_columns(
                pl.col("assigned_gpu_node").mul(8 * 24).alias("assigned_gpu_hour"),
            )
        )
        _overall_export_df = overall_summarize(
            df=all_runs_df,
            companies_config=companies_config,
            target_date=target_date,
        ).drop("assigned_gpu_node", "assigned_gpu_hour", "days")
        # join
        overall_export_df = (
            gpu_schedule_df.join(_overall_export_df, on=["company_name"], how="left")
            .select(
                pl.col("company_name").alias("企業名"),
                pl.col("total_gpu_hour").fill_null(0).alias("合計GPU使用時間"),
                pl.col("utilization_rate").fill_null(0).alias("GPU稼働率(%)"),
                pl.col("average_gpu_utilization")
                .fill_null(0)
                .alias("平均GPUパフォーマンス率(%)"),
                pl.col("max_gpu_utilization")
                .fill_null(0)
                .alias("最大GPUパフォーマンス率(%)"),
                pl.col("average_gpu_memory")
                .fill_null(0)
                .alias("平均GPUメモリ利用率(%)"),
                pl.col("max_gpu_memory").fill_null(0).alias("最大GPUメモリ利用率(%)"),
                pl.col("n_runs").fill_null(0),
                pl.col("duration_hour").fill_null(0),
                pl.col("assigned_gpu_node").fill_null(0),
                pl.col("assigned_gpu_hour").fill_null(0),
            )
            .sort("企業名")
        )
        wandb.log(
            {"overall_gpu_usage": wandb.Table(data=overall_export_df.to_pandas())}
        )
        ### Monthly tables
        gpu_schedule_df = (
            get_whole_gpu_schedule(
                companies_config=companies_config, target_date=target_date
            )
            .with_columns(
                pl.col("date").dt.strftime("%Y-%m").alias("year_month"),
                pl.col("date").min().alias("start_date"),
            )
            .group_by("year_month", "company_name")
            .agg(
                pl.col("assigned_gpu_node").sum(),
                pl.col("date").count().alias("days"),
            )
            .with_columns(
                pl.col("assigned_gpu_node").mul(8 * 24).alias("assigned_gpu_hour"),
            )
        )
        _monthly_export_df = monthly_summarize(
            df=all_runs_df,
            companies_config=companies_config,
            target_date=target_date,
        ).drop("assigned_gpu_node", "assigned_gpu_hour", "days")
        # join
        monthly_export_df = (
            gpu_schedule_df.join(
                _monthly_export_df, on=["year_month", "company_name"], how="left"
            )
            .select(
                pl.col("company_name").alias("企業名"),
                pl.col("year_month").alias("年月"),
                pl.col("total_gpu_hour").fill_null(0).alias("合計GPU使用時間"),
                pl.col("utilization_rate").fill_null(0).alias("GPU稼働率(%)"),
                pl.col("average_gpu_utilization")
                .fill_null(0)
                .alias("平均GPUパフォーマンス率(%)"),
                pl.col("max_gpu_utilization")
                .fill_null(0)
                .alias("最大GPUパフォーマンス率(%)"),
                pl.col("average_gpu_memory")
                .fill_null(0)
                .alias("平均GPUメモリ利用率(%)"),
                pl.col("max_gpu_memory").fill_null(0).alias("最大GPUメモリ利用率(%)"),
                pl.col("n_runs").fill_null(0),
                pl.col("duration_hour").fill_null(0),
                pl.col("assigned_gpu_node").fill_null(0),
                pl.col("assigned_gpu_hour").fill_null(0),
            )
            .sort("年月", descending=True)
            .sort("企業名")
        )
        wandb.log(
            {"monthly_gpu_usage": wandb.Table(data=monthly_export_df.to_pandas())}
        )
    ### Daily tables
    daily_summary_df = daily_summarize(df=all_runs_df)
    for company_name in daily_df["company_name"].unique():
        __daily_export_df = daily_df.filter(
            pl.col("company_name") == company_name
        )  # Basis
        company_df = daily_summary_df.filter(
            pl.col("company_name") == company_name
        )  # Records
        if company_df.is_empty():
            _daily_export_df = __daily_export_df.with_columns(
                pl.lit(0).cast(pl.Float64).alias("n_runs"),
                (pl.col("assigned_gpu_node") * 24 * 8)
                .cast(pl.Float64)
                .alias("assigned_gpu_hour"),
                pl.lit(0).cast(pl.Float64).alias("duration_hour"),
                pl.lit(0).cast(pl.Float64).alias("total_gpu_hour"),
                pl.lit(0).cast(pl.Float64).alias("utilization_rate"),
                pl.lit(0).cast(pl.Float64).alias("no_cap_utilization_rate"),
                pl.lit(None).cast(pl.Float64).alias("average_gpu_utilization"),
                pl.lit(None).cast(pl.Float64).alias("max_gpu_utilization"),
                pl.lit(None).cast(pl.Float64).alias("average_gpu_memory"),
                pl.lit(None).cast(pl.Float64).alias("max_gpu_memory"),
            )
        else:
            _daily_export_df = __daily_export_df.join(
                company_df,
                on=["date", "company_name"],
                how="left",
            ).with_columns(
                (pl.col("assigned_gpu_node") * 24 * 8)
                .cast(pl.Float64)
                .alias("assigned_gpu_hour"),
            )
        # Export
        daily_export_df = _daily_export_df.select(
            pl.col("company_name").alias("企業名"),
            pl.col("date").dt.strftime("%Y-%m-%d").alias("日付"),
            pl.col("total_gpu_hour").fill_null(0).alias("合計GPU使用時間"),
            pl.col("utilization_rate").fill_null(0).alias("GPU稼働率(%)"),
            pl.col("average_gpu_utilization")
            .fill_null(0)
            .alias("平均GPUパフォーマンス率(%)"),
            pl.col("max_gpu_utilization")
            .fill_null(0)
            .alias("最大GPUパフォーマンス率(%)"),
            pl.col("average_gpu_memory").fill_null(0).alias("平均GPUメモリ利用率(%)"),
            pl.col("max_gpu_memory").fill_null(0).alias("最大GPUメモリ利用率(%)"),
            pl.col("n_runs").fill_null(0),
            pl.col("duration_hour").fill_null(0),
            pl.col("no_cap_utilization_rate").fill_null(0),
            pl.col("assigned_gpu_node").fill_null(0),
            pl.col("assigned_gpu_hour").fill_null(0),
        ).sort("日付", descending=True)
        with wandb.init(
            entity=path_to_dashboard.entity,
            project=path_to_dashboard.project,
            name=f"Tables_{target_date_str}",
            job_type="update-table",
            tags=[company_name, tag_for_latest],
        ) as run:
            over_100_df = daily_export_df.filter(
                pl.col("no_cap_utilization_rate") > 100
            )
            if not over_100_df.is_empty():
                alert_text = str(over_100_df.to_pandas().to_dict(orient="records"))
                print(alert_text)
                wandb.alert(
                    title="Utilization rate too high",
                    text=alert_text,
                )
            wandb.log(
                {
                    "company_daily_gpu_usage": wandb.Table(
                        data=daily_export_df.to_pandas()
                    ),
                    "company_daily_gpu_usage_within_30days": wandb.Table(
                        data=daily_export_df.head(30).to_pandas()
                    ),
                }
            )
    return {}


def remove_project_tags(
    entity: str, project: str, delete_tags: list[str], head: int
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
    run_paths = get_run_paths(entity=entity, project=project)[:head]
    assert run_paths, f"run_paths: {run_paths}"
    for run_path in tqdm(run_paths):
        print(run_path)
        run = api.run(path=run_path)
        old_tags = run.tags
        new_tags = [tag for tag in old_tags if tag not in delete_tags]
        run.tags = new_tags
        run.update()
