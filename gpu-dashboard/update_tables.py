import datetime as dt

import polars as pl
import wandb

from blank_table import BlankTable
from config import CONFIG

GPU_PER_NODE = 8
HOURS_PER_DAY = 24
MAX_PERCENT = 100


def fillna_round(srs: pl.Series) -> pl.Series:
    return srs.fill_null(0).fill_nan(0).round(1)


TMP_COLS = (
    pl.when(pl.col("average_gpu_utilization").is_not_null())
    .then(pl.col("duration_hour"))
    .otherwise(None)
    .alias("metrics_hour"),
    (pl.col("average_gpu_utilization") * pl.col("duration_hour")).alias(
        "sum_gpu_utilization"
    ),
    (pl.col("average_gpu_memory") * pl.col("duration_hour")).alias("sum_gpu_memory"),
)

AGG_COLS = (
    pl.col("assigned_gpu_node")
    .first()
    .mul(GPU_PER_NODE * HOURS_PER_DAY)
    .alias("assigned_gpu_hour"),
    pl.col("metrics_hour").sum().alias("total_metrics_hour"),
    pl.col("sum_gpu_utilization").sum(),
    pl.col("max_gpu_utilization").max(),
    pl.col("sum_gpu_memory").sum(),
    pl.col("max_gpu_memory").max(),
    pl.col("run_id").n_unique().alias("n_runs"),
    pl.col("assigned_gpu_node").first(),
)

METRICS_COLS = (
    # apply cap to utilization_rate
    pl.when(pl.col("total_gpu_hour") > pl.col("assigned_gpu_hour"))
    .then(MAX_PERCENT)
    .otherwise(
        (pl.col("total_gpu_hour") / pl.col("assigned_gpu_hour")).mul(MAX_PERCENT)
    )
    .alias("utilization_rate"),
    # average of metrics
    (pl.col("sum_gpu_utilization") / pl.col("total_metrics_hour")).alias(
        "average_gpu_utilization"
    ),
    (pl.col("sum_gpu_memory") / pl.col("total_metrics_hour")).alias(
        "average_gpu_memory"
    ),
)

SELECT_COLS = (
    pl.col("total_gpu_hour").pipe(fillna_round).alias("合計GPU使用時間(h)"),
    pl.col("utilization_rate").pipe(fillna_round).alias("GPU稼働率(%)"),
    pl.col("average_gpu_utilization")
    .pipe(fillna_round)
    .alias("平均GPUパフォーマンス率(%)"),
    pl.col("max_gpu_utilization")
    .pipe(fillna_round)
    .alias("最大GPUパフォーマンス率(%)"),
    pl.col("average_gpu_memory").pipe(fillna_round).alias("平均GPUメモリ利用率(%)"),
    pl.col("max_gpu_memory").pipe(fillna_round).alias("最大GPUメモリ利用率(%)"),
    pl.col("n_runs"),
    pl.col("assigned_gpu_node"),
    pl.col("assigned_gpu_hour"),
    pl.col("_total_gpu_hour"),
    pl.col("total_metrics_hour"),
)


def update_tables(all_runs_df: pl.DataFrame, target_date: dt.date) -> None:
    gpu_overall_table = agg_overall(all_runs_df=all_runs_df, target_date=target_date)
    gpu_monthly_table = agg_monthly(all_runs_df=all_runs_df, target_date=target_date)
    gpu_daily_table = agg_daily(all_runs_df=all_runs_df, target_date=target_date)
    target_date_str = target_date.strftime("%Y-%m-%d")
    update_overall(
        gpu_overall_table=gpu_overall_table,
        gpu_monthly_table=gpu_monthly_table,
        target_date_str=target_date_str,
    )
    update_companies(
        gpu_daily_table=gpu_daily_table,
        target_date_str=target_date_str,
    )
    return None


def add_team(all_runs_df: pl.DataFrame, team_table: pl.DataFrame) -> pl.DataFrame:
    all_runs_df_without_team = all_runs_df.join(
        team_table, left_on="company_name", right_on="team", how="left"
    ).drop("company_name", "assigned_gpu_node")
    return all_runs_df_without_team


def agg_gpu_hour(
    all_runs_df: pl.DataFrame, target_date: dt.date, keys: list[str]
) -> pl.DataFrame:
    bt = BlankTable(target_date=target_date)
    all_runs_df_without_team = add_team(
        all_runs_df=all_runs_df, team_table=bt.team_table
    )
    gpu_hour_df = (
        bt.daily_table.join(
            all_runs_df_without_team,
            on=["company", "date"],
            how="left",
        )
        .with_columns((pl.col("duration_hour") * pl.col("gpu_count")).alias("gpu_hour"))
        .group_by("company", "date")
        .agg(
            pl.col("gpu_hour").sum().pipe(fillna_round).alias("total_gpu_hour"),
            pl.col("assigned_gpu_node")
            .first()
            .mul(GPU_PER_NODE * HOURS_PER_DAY)
            .alias("assigned_gpu_hour"),
        )
        .with_columns(
            pl.col("total_gpu_hour").alias("_total_gpu_hour"),
            pl.when(pl.col("total_gpu_hour") > pl.col("assigned_gpu_hour"))
            .then(pl.col("assigned_gpu_hour"))
            .otherwise(pl.col("total_gpu_hour"))
            .alias("total_gpu_hour"),
        )
        .drop("assigned_gpu_hour")
        .with_columns(pl.col("date").dt.strftime("%Y-%m").alias("year_month"))
        .group_by(keys)
        .agg(pl.col("total_gpu_hour").sum(), pl.col("_total_gpu_hour").sum())
        .sort(["company"])
    )

    return gpu_hour_df


def agg_daily(all_runs_df: pl.DataFrame, target_date: dt.date) -> pl.DataFrame:
    bt = BlankTable(target_date=target_date)
    all_runs_df_without_team = add_team(
        all_runs_df=all_runs_df, team_table=bt.team_table
    )
    keys = ["company", "date"]

    gpu_daily_table = (
        bt.daily_table.join(
            all_runs_df_without_team,
            on=keys,
            how="left",
        )
        .with_columns(*TMP_COLS)
        .group_by(keys)
        .agg(*AGG_COLS)
        .join(
            agg_gpu_hour(all_runs_df=all_runs_df, target_date=target_date, keys=keys),
            on=keys,
            how="left",
        )
        .with_columns(*METRICS_COLS)
        .select(
            pl.col("company").alias("企業名"),
            pl.col("date").dt.strftime("%Y-%m-%d").alias("日付"),
            *SELECT_COLS,
        )
        .sort(["日付"], descending=True)
        .sort(["企業名"])
    )

    return gpu_daily_table


def agg_monthly(all_runs_df: pl.DataFrame, target_date: dt.date) -> pl.DataFrame:
    bt = BlankTable(target_date=target_date)
    all_runs_df_without_team = add_team(
        all_runs_df=all_runs_df, team_table=bt.team_table
    ).with_columns(pl.col("date").dt.strftime("%Y-%m").alias("year_month"))
    keys = ["company", "year_month"]

    gpu_monthly_table = (
        bt.monthy_table.join(
            all_runs_df_without_team,
            on=keys,
            how="left",
        )
        .with_columns(*TMP_COLS)
        .group_by(keys)
        .agg(*AGG_COLS)
        .join(
            agg_gpu_hour(all_runs_df=all_runs_df, target_date=target_date, keys=keys),
            on=keys,
            how="left",
        )
        .with_columns(*METRICS_COLS)
        .select(
            pl.col("company").alias("企業名"),
            pl.col("year_month").alias("年月"),
            *SELECT_COLS,
        )
        .sort(["年月"], descending=True)
        .sort(["企業名"])
    )

    return gpu_monthly_table


def agg_overall(all_runs_df: pl.DataFrame, target_date: dt.date) -> pl.DataFrame:
    bt = BlankTable(target_date=target_date)
    all_runs_df_without_team = add_team(
        all_runs_df=all_runs_df, team_table=bt.team_table
    )
    keys = ["company"]

    gpu_overall_table = (
        bt.overall_table.join(
            all_runs_df_without_team,
            on=keys,
            how="left",
        )
        .with_columns(*TMP_COLS)
        .group_by(keys)
        .agg(*AGG_COLS)
        .join(
            agg_gpu_hour(all_runs_df=all_runs_df, target_date=target_date, keys=keys),
            on=keys,
            how="left",
        )
        .with_columns(*METRICS_COLS)
        .select(pl.col("company").alias("企業名"), *SELECT_COLS)
        .sort(["企業名"])
    )

    return gpu_overall_table


def update_overall(
    gpu_overall_table: pl.DataFrame,
    gpu_monthly_table: pl.DataFrame,
    target_date_str: str,
) -> None:
    with wandb.init(
        entity=CONFIG.dashboard.entity,
        project=CONFIG.dashboard.project,
        name=f"Tables_{target_date_str}",
        job_type="update-table",
        tags=["overall", CONFIG.dashboard.tag_for_latest],
    ) as run:
        wandb.log(
            {
                "overall_gpu_usage": wandb.Table(data=gpu_overall_table.to_pandas()),
                "monthly_gpu_usage": wandb.Table(data=gpu_monthly_table.to_pandas()),
            }
        )
    return None


def update_companies(
    gpu_daily_table: pl.DataFrame,
    target_date_str: str,
) -> None:
    for company in sorted(gpu_daily_table["企業名"].unique()):
        gpu_daily_company_table = gpu_daily_table.filter(pl.col("企業名") == company)
        with wandb.init(
            entity=CONFIG.dashboard.entity,
            project=CONFIG.dashboard.project,
            name=f"Tables_{target_date_str}",
            job_type="update-table",
            tags=[company, CONFIG.dashboard.tag_for_latest],
        ) as run:
            limit = 30
            wandb.log(
                {
                    "company_daily_gpu_usage": wandb.Table(
                        data=gpu_daily_company_table.to_pandas()
                    ),
                    f"company_daily_gpu_usage_within_{limit}days": wandb.Table(
                        data=gpu_daily_company_table.head(limit).to_pandas()
                    ),
                }
            )

    return None
