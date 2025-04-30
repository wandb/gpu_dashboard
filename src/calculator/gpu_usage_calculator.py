import datetime as dt
import polars as pl
import wandb
from typing import List
from src.calculator.blank_table import BlankTable
from src.utils.config import CONFIG
from src.calculator.common import (fillna_round, OVERALL_SCHEMA, MONTHLY_SCHEMA, WEEKLY_SCHEMA, 
                                   DAILY_SCHEMA, SUMMARY_SCHEMA, GPU_PER_NODE, 
                                   HOURS_PER_DAY, TMP_COLS, AGG_COLS, METRICS_COLS, SELECT_COLS)

class GPUUsageCalculator:
    def __init__(self, all_runs_df: pl.DataFrame, date_range: List):
        if CONFIG.ignore_deleted_run:
            self.all_runs_df = all_runs_df.filter(pl.col("run_exists") != "deleted")
        else:
            self.all_runs_df = all_runs_df
        self.start_date = dt.datetime.strptime(date_range[0], "%Y-%m-%d").date()
        self.end_date = dt.datetime.strptime(date_range[1], "%Y-%m-%d").date()
        self.bt = BlankTable(self.end_date)

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
        
        daily_table = self.bt.daily_table
        join_keys = ["company", "date"]
        
        gpu_hour_df = (
            daily_table.join(
                all_runs_df_without_team,
                on=join_keys,
                how="left",
            )
            .with_columns((pl.col("duration_hour") * pl.col("gpu_count")).alias("gpu_hour"))
            .group_by(join_keys)
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
        )
        
        # 月次データ用の処理を追加
        if "year_month" in keys:
            gpu_hour_df = gpu_hour_df.with_columns(
                pl.col("date").dt.strftime("%Y-%m").alias("year_month")
            )
        elif "week_start" in keys:
            gpu_hour_df = gpu_hour_df.with_columns(
                (pl.col("date") - pl.duration(days=pl.col("date").dt.weekday() % 7)).alias("week_start")
            )
        
        gpu_hour_df = (
            gpu_hour_df.group_by(keys)
            .agg(pl.col("total_gpu_hour").sum(), pl.col("_total_gpu_hour").sum())
            .select(*keys, "total_gpu_hour", "_total_gpu_hour")
            .sort(["company"])
        )
        return gpu_hour_df

    def agg_daily(self) -> pl.DataFrame:
        if self.all_runs_df.is_empty():
            return pl.DataFrame(schema=DAILY_SCHEMA)
        
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
            return pl.DataFrame(schema=WEEKLY_SCHEMA)
        
        # end_dateの週の開始日（月曜日）を計算
        target_week_start = self.end_date - dt.timedelta(days=self.end_date.weekday())
        
        all_runs_df_without_team = self.add_team().with_columns(
            (pl.col("date") - pl.duration(days=(pl.col("date").dt.weekday() % 7))).alias("week_start")
        )
        keys = ["company", "week_start"]

        gpu_weekly_table = (
            self.bt.weekly_table.join(
                all_runs_df_without_team.filter(pl.col("week_start") < target_week_start),
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
                pl.col("week_start").dt.strftime("%Y-%m-%d").alias("週開始日"),
                *SELECT_COLS,
            )
            .sort(["週開始日"], descending=True)
            .sort(["企業名"])
        )

        return gpu_weekly_table

    def agg_monthly(self) -> pl.DataFrame:
        if self.all_runs_df.is_empty():
            return pl.DataFrame(schema=MONTHLY_SCHEMA)
        
        all_runs_df_without_team = self.add_team().with_columns(pl.col("date").dt.strftime("%Y-%m").alias("year_month"))
        keys = ["company", "year_month"]

        gpu_monthly_table = (
            self.bt.monthly_table.join(
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
            return pl.DataFrame(schema=OVERALL_SCHEMA)
        
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

    def update_overall(self, gpu_overall_table: pl.DataFrame, gpu_monthly_table: pl.DataFrame, gpu_weekly_table: pl.DataFrame, gpu_daily_table: pl.DataFrame):
        with wandb.init(
            entity=CONFIG.dashboard.entity,
            project=CONFIG.dashboard.project,
            name=f"Tables_{self.end_date}",
            job_type="update-table",
            tags=["overall", CONFIG.dashboard.tag_for_latest],
        ) as run:
            wandb.log(
                {
                    "overall_gpu_usage": wandb.Table(data=gpu_overall_table.to_pandas()),
                    "monthly_gpu_usage": wandb.Table(data=gpu_monthly_table.to_pandas()),
                    "weekly_gpu_usage": wandb.Table(data=gpu_weekly_table.to_pandas()),
                    "daily_gpu_usage": wandb.Table(data=gpu_daily_table.to_pandas()),
                }
            )
            if gpu_overall_table.is_empty():
                wandb.log({"warning": "No data available for overall, monthly, and weekly tables"})

    def update_companies(self, gpu_daily_table: pl.DataFrame, gpu_weekly_table: pl.DataFrame, gpu_summary_table: pl.DataFrame, cpu_weekly_table: pl.DataFrame):
        limit = 30

        if gpu_daily_table.is_empty():
            print("Warning: No data to update for companies.")

        for company_info in CONFIG.companies:
            company = company_info['company']
            
            is_gpu_assigned = False
            for i, schedule in enumerate(company_info['schedule']):
                schedule_start = dt.datetime.strptime(schedule['date'], "%Y-%m-%d").date()
                schedule_end = dt.datetime.strptime(company_info['schedule'][i+1]['date'], "%Y-%m-%d").date() if i+1 < len(company_info['schedule']) else dt.date.max

                if (schedule_start <= self.end_date and self.start_date < schedule_end) and schedule['assigned_gpu_node'] > 0:
                    is_gpu_assigned = True
                    break
            
            if not is_gpu_assigned:
                continue

            gpu_daily_company_table = gpu_daily_table.filter(pl.col("企業名") == company)
            gpu_weekly_company_table = gpu_weekly_table.filter(pl.col("企業名") == company)
            gpu_summary_company_table = gpu_summary_table.filter(pl.col("company_name") == company)
            
            # CPU週次テーブルの追加（syntheticgestalt-geniacの場合のみ）
            cpu_weekly_company_table = cpu_weekly_table.filter(pl.col("企業名") == company) if company == "syntheticgestalt-geniac" else None
            
            with wandb.init(
                entity=CONFIG.dashboard.entity,
                project=CONFIG.dashboard.project,
                name=f"Tables_{self.end_date}",
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

                # CPU週次テーブルの追加（syntheticgestalt-geniacの場合のみ）
                if company == "syntheticgestalt-geniac" and cpu_weekly_company_table is not None and not cpu_weekly_company_table.is_empty():
                    data_to_log["company_weekly_cpu_usage"] = wandb.Table(data=cpu_weekly_company_table.to_pandas())
                    data_to_log[f"company_weekly_cpu_usage_within_{limit//7}weeks"] = wandb.Table(data=cpu_weekly_company_table.head(limit//7).to_pandas())
                
                wandb.log(data_to_log)

    def agg_summary(self) -> pl.DataFrame:
        if self.all_runs_df.is_empty():
            return pl.DataFrame(schema=SUMMARY_SCHEMA)
        
        start_date = self.end_date - dt.timedelta(days=(self.end_date.weekday() + 1) % 7)
        end_date = start_date + dt.timedelta(days=6)
        df_filtered = self.all_runs_df.filter(
            (pl.col('date') >= start_date) & (pl.col('date') <= end_date) & (pl.col('run_exists') != "deleted")
        )
        
        summary = (
            df_filtered
            .with_columns([
                (pl.col('duration_hour') * pl.col('gpu_count')).alias('weighted_duration'),
                (pl.col('gpu_count') >= 9).alias('is_master_node'),
                pl.col('tags').map_elements(lambda x: any(tag.strip('[]"\'') in CONFIG.ignore_tags for tag in eval(x)), return_dtype=pl.Boolean).alias('has_ignore_tag')
            ])
            # Ensure uniqueness by run_id
            .group_by(['company_name', 'project', 'run_id'])
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

    def agg_cpu_weekly(self) -> pl.DataFrame:
        if self.all_runs_df.is_empty():
            return pl.DataFrame(schema=WEEKLY_SCHEMA)
        
        target_week_start = self.end_date - dt.timedelta(days=self.end_date.weekday())
        
        all_runs_df_without_team = self.add_team().with_columns(
            (pl.col("date") - pl.duration(days=(pl.col("date").dt.weekday() % 7))).alias("week_start")
        )
        keys = ["company", "week_start"]

        HOURS_PER_WEEK = 24 * 7
        TOTAL_CPUS = 2704

        cpu_weekly_table = (
            self.bt.weekly_table.join(
                all_runs_df_without_team.filter((pl.col("week_start") < target_week_start) & (pl.col("company") == "syntheticgestalt-geniac")),
                on=keys,
                how="left",
            )
            .group_by(keys)
            .agg(
                (pl.col("duration_hour") * pl.col("cpu_count")).sum().alias("total_cpu_hour"),
                pl.col("run_id").n_unique().alias("n_runs"),
            )
            .with_columns(
                pl.col("total_cpu_hour").pipe(fillna_round).alias("合計CPU使用時間(h)"),
                pl.lit(TOTAL_CPUS).alias("assigned_cpus"),
                (pl.lit(TOTAL_CPUS) * HOURS_PER_WEEK).alias("assigned_cpu_hour"),
                (pl.col("total_cpu_hour") / (pl.lit(TOTAL_CPUS) * HOURS_PER_WEEK) * 100).pipe(fillna_round).alias("CPU稼働率(%)"),
            )
            .select(
                pl.col("company").alias("企業名"),
                pl.col("week_start").dt.strftime("%Y-%m-%d").alias("週開始日"),
                "合計CPU使用時間(h)",
                "CPU稼働率(%)",
                "n_runs",
                pl.col("total_cpu_hour").alias("_total_cpu_hour"),
                "assigned_cpus",
                "assigned_cpu_hour",
            )
            .sort(["週開始日"], descending=True)
            .sort(["企業名"])
            .filter(pl.col("企業名") == "syntheticgestalt-geniac")
        )

        return cpu_weekly_table

    def update_tables(self):
        gpu_overall_table = self.agg_overall()
        gpu_monthly_table = self.agg_monthly()
        gpu_weekly_table = self.agg_weekly()
        gpu_daily_table = self.agg_daily()
        gpu_summary_table = self.agg_summary()
        cpu_weekly_table = self.agg_cpu_weekly()
        self.update_overall(gpu_overall_table, gpu_monthly_table, gpu_weekly_table, gpu_daily_table)
        self.update_companies(gpu_daily_table, gpu_weekly_table, gpu_summary_table, cpu_weekly_table)

if __name__ == "__main__":
    df = pl.read_csv('dev/all_runs_data.csv', schema={"date": pl.Date, "company_name": pl.Utf8, "project": pl.Utf8, "run_id": pl.Utf8, "tags": pl.Utf8, 
                                                     "created_at": pl.Datetime, "updated_at": pl.Datetime, "state": pl.Utf8, "duration_hour": pl.Float64, 
                                                     "gpu_count": pl.Int64, "cpu_count": pl.Int64, "average_gpu_utilization": pl.Float64, "average_gpu_memory": pl.Float64, 
                                                     "max_gpu_utilization": pl.Float64, "max_gpu_memory": pl.Float64, "host_name": pl.Utf8, "logged_at": pl.Datetime, "run_exists": pl.Utf8})
    date_range = ["2024-10-25", "2024-10-25"]
    guc = GPUUsageCalculator(df, date_range)
    # guc.update_tables()
    gpu_overall_table = guc.agg_overall()
    gpu_monthly_table = guc.agg_monthly()
    gpu_weekly_table = guc.agg_weekly()
    gpu_daily_table = guc.agg_daily()
    gpu_summary_table = guc.agg_summary()
    cpu_weekly_table = guc.agg_cpu_weekly()
    gpu_overall_table.write_csv("dev/gpu_overall_table.csv")
    gpu_monthly_table.write_csv("dev/gpu_monthly_table.csv")
    gpu_weekly_table.write_csv("dev/gpu_weekly_table.csv")
    gpu_daily_table.write_csv("dev/gpu_daily_table.csv")
    gpu_summary_table.write_csv("dev/gpu_summary_table.csv")
    cpu_weekly_table.write_csv("dev/cpu_weekly_table.csv")
