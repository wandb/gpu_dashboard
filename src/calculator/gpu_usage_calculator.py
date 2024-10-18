import datetime as dt
import polars as pl
import wandb
from src.calculator.blank_table import BlankTable
from src.utils.config import CONFIG

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
    pl.when(pl.col("total_gpu_hour") > pl.col("assigned_gpu_hour"))
    .then(MAX_PERCENT)
    .otherwise(
        (pl.col("total_gpu_hour") / pl.col("assigned_gpu_hour")).mul(MAX_PERCENT)
    )
    .alias("utilization_rate"),
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

class GPUUsageCalculator:

    def __init__(self, all_runs_df: pl.DataFrame, target_date: str):
        self.all_runs_df = all_runs_df
        self.target_date = dt.datetime.strptime(target_date, "%Y-%m-%d").date()
        self.bt = BlankTable(target_date=self.target_date)

    def add_team(self) -> pl.DataFrame:
        if self.all_runs_df.is_empty():
            return pl.DataFrame(schema=self.bt.team_table.schema)
        return self.all_runs_df.join(
            self.bt.team_table, left_on="company_name", right_on="team", how="left"
        ).drop("company_name", "assigned_gpu_node")

    def agg_gpu_hour(self, keys: list[str]) -> pl.DataFrame:
        if self.all_runs_df.is_empty():
            return pl.DataFrame(schema={k: pl.Utf8 for k in keys} | {"total_gpu_hour": pl.Float64, "_total_gpu_hour": pl.Float64})
        
        all_runs_df_without_team = self.add_team()
        gpu_hour_df = (
            self.bt.daily_table.join(
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
            .select(*keys, "total_gpu_hour", "_total_gpu_hour")
            .sort(["company"])
        )
        return gpu_hour_df

    def agg_daily(self) -> pl.DataFrame:
        if self.all_runs_df.is_empty():
            return pl.DataFrame(schema={"企業名": pl.Utf8, "日付": pl.Utf8, "合計GPU使用時間(h)": pl.Float64, "GPU稼働率(%)": pl.Float64, 
                                        "平均GPUパフォーマンス率(%)": pl.Float64, "最大GPUパフォーマンス率(%)": pl.Float64, 
                                        "平均GPUメモリ利用率(%)": pl.Float64, "最大GPUメモリ利用率(%)": pl.Float64, 
                                        "n_runs": pl.Int64, "assigned_gpu_node": pl.Int64, "assigned_gpu_hour": pl.Float64, 
                                        "_total_gpu_hour": pl.Float64, "total_metrics_hour": pl.Float64})
        
        all_runs_df_without_team = self.add_team()
        keys = ["company", "date"]

        gpu_daily_table = (
            self.bt.daily_table.join(
                all_runs_df_without_team,
                on=keys,
                how="left",
            )
            .with_columns(*TMP_COLS)
            .group_by(keys)
            .agg(*AGG_COLS)
            .join(
                self.agg_gpu_hour(keys=keys),
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

    def agg_weekly(self) -> pl.DataFrame:
        if self.all_runs_df.is_empty():
            return pl.DataFrame(schema={"企業名": pl.Utf8, "日付": pl.Utf8, "合計GPU使用時間(h)": pl.Float64, "GPU稼働率(%)": pl.Float64, 
                                        "平均GPUパフォーマンス率(%)": pl.Float64, "最大GPUパフォーマンス率(%)": pl.Float64, 
                                        "平均GPUメモリ利用率(%)": pl.Float64, "最大GPUメモリ利用率(%)": pl.Float64, 
                                        "n_runs": pl.Int64, "assigned_gpu_node": pl.Int64, "assigned_gpu_hour": pl.Float64, 
                                        "_total_gpu_hour": pl.Float64, "total_metrics_hour": pl.Float64})
        
        all_runs_df_without_team = self.add_team()
        keys = ["company", "date"]

        # 日曜日を週の開始日として設定
        all_runs_df_without_team = all_runs_df_without_team.with_columns(
            (pl.col("date") - pl.duration(days=pl.col("date").dt.weekday())).alias("date")
        )

        gpu_weekly_table = (
            self.bt.daily_table.with_columns(
                (pl.col("date") - pl.duration(days=pl.col("date").dt.weekday())).alias("date")
            ).join(
                all_runs_df_without_team,
                on=keys,
                how="left",
            )
            .with_columns(*TMP_COLS)
            .group_by(keys)
            .agg(*AGG_COLS)
            .join(
                self.agg_gpu_hour(keys=keys),
                on=keys,
                how="left",
            )
            .with_columns(*METRICS_COLS)
            .select(
                pl.col("company").alias("企業名"),
                pl.col("date").dt.strftime("%Y-%m-%d").alias("週開始日"),
                *SELECT_COLS,
            )
            .sort(["週開始日"], descending=True)
            .sort(["企業名"])
        )

        return gpu_weekly_table

    def agg_monthly(self) -> pl.DataFrame:
        if self.all_runs_df.is_empty():
            return pl.DataFrame(schema={"企業名": pl.Utf8, "日付": pl.Utf8, "合計GPU使用時間(h)": pl.Float64, "GPU稼働率(%)": pl.Float64, 
                                        "平均GPUパフォーマンス率(%)": pl.Float64, "最大GPUパフォーマンス率(%)": pl.Float64, 
                                        "平均GPUメモリ利用率(%)": pl.Float64, "最大GPUメモリ利用率(%)": pl.Float64, 
                                        "n_runs": pl.Int64, "assigned_gpu_node": pl.Int64, "assigned_gpu_hour": pl.Float64, 
                                        "_total_gpu_hour": pl.Float64, "total_metrics_hour": pl.Float64})
        
        all_runs_df_without_team = self.add_team().with_columns(pl.col("date").dt.strftime("%Y-%m").alias("year_month"))
        keys = ["company", "year_month"]

        gpu_monthly_table = (
            self.bt.monthy_table.join(
                all_runs_df_without_team,
                on=keys,
                how="left",
            )
            .with_columns(*TMP_COLS)
            .group_by(keys)
            .agg(*AGG_COLS)
            .join(
                self.agg_gpu_hour(keys=keys),
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

    def agg_overall(self) -> pl.DataFrame:
        if self.all_runs_df.is_empty():
            return pl.DataFrame(schema={"企業名": pl.Utf8, "日付": pl.Utf8, "合計GPU使用時間(h)": pl.Float64, "GPU稼働率(%)": pl.Float64, 
                                        "平均GPUパフォーマンス率(%)": pl.Float64, "最大GPUパフォーマンス率(%)": pl.Float64, 
                                        "平均GPUメモリ利用率(%)": pl.Float64, "最大GPUメモリ利用率(%)": pl.Float64, 
                                        "n_runs": pl.Int64, "assigned_gpu_node": pl.Int64, "assigned_gpu_hour": pl.Float64, 
                                        "_total_gpu_hour": pl.Float64, "total_metrics_hour": pl.Float64})
        
        all_runs_df_without_team = self.add_team()
        keys = ["company"]

        gpu_overall_table = (
            self.bt.overall_table.join(
                all_runs_df_without_team,
                on=keys,
                how="left",
            )
            .with_columns(*TMP_COLS)
            .group_by(keys)
            .agg(*AGG_COLS)
            .join(
                self.agg_gpu_hour(keys=keys),
                on=keys,
                how="left",
            )
            .with_columns(*METRICS_COLS)
            .select(pl.col("company").alias("企業名"), *SELECT_COLS)
            .sort(["企業名"])
        )

        return gpu_overall_table

    def update_overall(self, gpu_overall_table: pl.DataFrame, gpu_monthly_table: pl.DataFrame, gpu_weekly_table: pl.DataFrame):
        with wandb.init(
            entity=CONFIG.dashboard.entity,
            project=CONFIG.dashboard.project,
            name=f"Tables_{self.target_date}",
            job_type="update-table",
            tags=["overall", CONFIG.dashboard.tag_for_latest],
        ) as run:
            wandb.log(
                {
                    "overall_gpu_usage": wandb.Table(data=gpu_overall_table.to_pandas()),
                    "monthly_gpu_usage": wandb.Table(data=gpu_monthly_table.to_pandas()),
                    "weekly_gpu_usage": wandb.Table(data=gpu_weekly_table.to_pandas()),
                }
            )
            if gpu_overall_table.is_empty():
                wandb.log({"warning": "No data available for overall, monthly, and weekly tables"})

    def update_companies(self, gpu_daily_table: pl.DataFrame, gpu_weekly_table: pl.DataFrame, gpu_summary_table: pl.DataFrame):
        limit = 30

        if gpu_daily_table.is_empty():
            print("Warning: No data to update for companies.")

        for company_info in CONFIG.companies:
            company = company_info['company']
            gpu_daily_company_table = gpu_daily_table.filter(pl.col("企業名") == company)
            gpu_weekly_company_table = gpu_weekly_table.filter(pl.col("企業名") == company)
            gpu_summary_company_table = gpu_summary_table.filter(pl.col("company_name") == company)
            
            with wandb.init(
                entity=CONFIG.dashboard.entity,
                project=CONFIG.dashboard.project,
                name=f"Tables_{self.target_date}",
                job_type="update-table",
                tags=[company, CONFIG.dashboard.tag_for_latest],
            ) as run:
                data_to_log = {}
                
                if gpu_daily_company_table.is_empty():
                    empty_df = pl.DataFrame({"column": []}).with_columns(pl.col("column").cast(pl.Utf8))
                    data_to_log = {
                        "company_daily_gpu_usage": wandb.Table(data=empty_df.to_pandas()),
                        f"company_daily_gpu_usage_within_{limit}days": wandb.Table(data=empty_df.to_pandas()),
                        "company_weekly_gpu_usage": wandb.Table(data=empty_df.to_pandas()),
                        f"company_weekly_gpu_usage_within_{limit//7}weeks": wandb.Table(data=empty_df.to_pandas()),
                        "company_summary": wandb.Table(data=empty_df.to_pandas()),
                        "warning": f"No data available for company: {company}"
                    }
                else:
                    data_to_log = {
                        "company_daily_gpu_usage": wandb.Table(data=gpu_daily_company_table.to_pandas()),
                        f"company_daily_gpu_usage_within_{limit}days": wandb.Table(data=gpu_daily_company_table.head(limit).to_pandas()),
                        "company_weekly_gpu_usage": wandb.Table(data=gpu_weekly_company_table.to_pandas()),
                        f"company_weekly_gpu_usage_within_{limit//7}weeks": wandb.Table(data=gpu_weekly_company_table.head(limit//7).to_pandas()),
                        "company_summary": wandb.Table(data=gpu_summary_company_table.to_pandas()),
                    }

                wandb.log(data_to_log)

                if CONFIG.enable_alert and not gpu_daily_company_table.is_empty():
                    latest_row_dict = gpu_daily_company_table.to_pandas().to_dict(
                        orient="records"
                    )[0]
                    threshold = 10
                    if latest_row_dict["GPU稼働率(%)"] < threshold:
                        wandb.alert(
                            title="Too low utilization rate found.",
                            text=company,
                        )

    def agg_summary(self) -> pl.DataFrame:
        if self.all_runs_df.is_empty():
            return pl.DataFrame(schema={"company_name": pl.Utf8, "project": pl.Utf8, "Total hours": pl.Float64, 
                                        "Total runs": pl.Int64, "master_node_runs": pl.Int64, 
                                        "overlap_runs": pl.Int64, "ignore_runs": pl.Int64})
        
        start_date = self.target_date - dt.timedelta(days=(self.target_date.weekday() + 7))
        end_date = start_date + dt.timedelta(days=7)
        df_filtered = self.all_runs_df.filter(
            (pl.col('date') >= start_date) & (pl.col('date') < end_date)
        )
        
        print(f"Analyzing data from {start_date} to {end_date}")
        
        summary = (
            df_filtered
            .with_columns([
                (pl.col('duration_hour') * pl.col('gpu_count')).alias('weighted_duration'),
                (pl.col('gpu_count') >= 9).alias('is_master_node'),
                pl.col('tags').apply(lambda x: any(tag.strip('[]"\'') in CONFIG.ignore_tags for tag in eval(x)), return_dtype=pl.Boolean).alias('has_ignore_tag')
            ])
            # Ensure uniqueness by run_id
            .groupby(['company_name', 'project', 'run_id'])
            .agg([
                pl.col('weighted_duration').sum(),
                pl.col('is_master_node').max(),
                pl.col('has_ignore_tag').max(),
                pl.col('created_at').min(),
                pl.col('updated_at').max(),
                pl.col('host_name').first()
            ])
            .sort(['company_name', 'project', 'host_name', 'created_at'])
            .with_columns([
                pl.col('updated_at').shift().over(['company_name', 'project', 'host_name']).alias('prev_updated_at')
            ])
            .with_columns([
                (pl.col('created_at') < pl.col('prev_updated_at')).alias('is_overlap')
            ])
            .group_by(['company_name', 'project'])
            .agg([
                pl.col('weighted_duration').sum().alias('Total hours'),
                pl.col('run_id').count().alias('Total runs'),
                pl.col('is_master_node').sum().alias('master_node_runs'),
                pl.col('is_overlap').sum().alias('overlap_runs'),
                pl.col('has_ignore_tag').sum().alias('ignore_runs')
            ])
            .with_columns([
                pl.col('Total hours').round(2),
                pl.col('Total runs').cast(pl.Int64),
                pl.col('master_node_runs').cast(pl.Int64),
                pl.col('overlap_runs').cast(pl.Int64),
                pl.col('ignore_runs').cast(pl.Int64)
            ])
            .sort(['company_name', 'project'])
        )
        
        return summary

    def update_tables(self):
        gpu_overall_table = self.agg_overall()
        gpu_monthly_table = self.agg_monthly()
        gpu_weekly_table = self.agg_weekly()
        gpu_daily_table = self.agg_daily()
        gpu_summary_table = self.agg_summary()
        self.update_overall(gpu_overall_table, gpu_monthly_table, gpu_weekly_table)
        self.update_companies(gpu_daily_table, gpu_weekly_table, gpu_summary_table)
