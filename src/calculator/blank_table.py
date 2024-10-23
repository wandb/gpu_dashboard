import datetime as dt

from easydict import EasyDict
import pandas as pd
import polars as pl

from src.utils.config import CONFIG


class BlankTable:
    def __init__(self, target_date: dt.date = dt.date.today()):
        self.target_date = target_date
        self.team_table = team_table()
        self.daily_table = daily_table(target_date=target_date)
        self.weekly_table = weekly_table(daily_table=self.daily_table)
        self.monthly_table = monthly_table(daily_table=self.daily_table)
        self.overall_table = overall_table(daily_table=self.daily_table)


def daily_table(target_date: dt.date) -> pl.DataFrame:
    """スケジュールから日次テーブルを作成"""
    df_list = []
    schedules = get_schedule()
    for s in schedules:
        # 主要な日付だけのdf
        minimum_schedule_df = pl.DataFrame(s.schedule).with_columns(
            pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"),
            pl.col("assigned_gpu_node").cast(pl.Float64),
        )
        
        # 日付範囲の開始日と終了日を決定
        start_date = min(minimum_schedule_df["date"].min(), target_date)
        end_date = max(minimum_schedule_df["date"].max(), target_date)
        
        # 日付を拡張
        date_df = pl.DataFrame(
            pl.date_range(
                start=start_date,
                end=end_date,
                interval="1d",
                eager=True,
            ).alias("date")
        )
        
        # 以下は変更なし
        company_schedule_df = (
            date_df.join(minimum_schedule_df, on=["date"], how="left")
            .with_columns(
                pl.lit(s.company).alias("company"),
                pl.col("assigned_gpu_node").forward_fill(),
            )
            .select(
                pl.col("company").cast(pl.Utf8),
                pl.col("date").cast(pl.Date),
                pl.col("assigned_gpu_node").cast(pl.Int64),
            ).filter(
                pl.col("assigned_gpu_node") > 0
            )
        )
        df_list.append(company_schedule_df)
    schedule_df = pl.concat(df_list)
    return schedule_df


def weekly_table(daily_table: pl.DataFrame) -> pl.DataFrame:
    """日次テーブルから週次テーブルを作成"""
    weekly_table = (
        daily_table.with_columns(
            (pl.col("date") - pl.duration(days=pl.col("date").dt.weekday())).alias("date")
        )
        .group_by("company", "date")
        .agg(pl.col("assigned_gpu_node").sum())
        .sort("date", "company")
        .select(
            pl.col("company").cast(pl.Utf8),
            pl.col("date").cast(pl.Date),
            pl.col("assigned_gpu_node").cast(pl.Int64),
        )
    )
    return weekly_table


def monthly_table(daily_table: pl.DataFrame) -> pl.DataFrame:
    """日次テーブルから月次テーブルを作成"""
    monthly_table = (
        daily_table.with_columns(
            pl.col("date").dt.strftime("%Y-%m").alias("year_month")
        )
        .group_by("company", "year_month")
        .agg(pl.col("assigned_gpu_node").sum())
        .sort("year_month", "company")
        .select(
            pl.col("company").cast(pl.Utf8),
            pl.col("year_month").cast(pl.Utf8),
            pl.col("assigned_gpu_node").cast(pl.Int64),
        )
    )
    return monthly_table


def overall_table(daily_table: pl.DataFrame) -> pl.DataFrame:
    """日次テーブルから全期間テーブルを作成"""
    overall_table = (
        daily_table.group_by("company")
        .agg(pl.col("assigned_gpu_node").sum())
        .sort("company")
        .select(
            pl.col("company").cast(pl.Utf8),
            pl.col("assigned_gpu_node").cast(pl.Int64),
        )
    )
    return overall_table


def get_schedule() -> list[EasyDict]:
    """configからスケジュールだけ取り出す"""
    comps = CONFIG.companies
    schedules = []
    keys = {"company", "schedule"}
    for comp in comps:
        s = EasyDict({k: v for k, v in comp.items() if k in keys})
        schedules.append(s)
    return schedules


def team_table() -> pl.DataFrame:
    """企業とチームの対応テーブルを作成"""
    df_list = []
    for c in CONFIG.companies:
        company = c["company"]
        teams = c["teams"]
        df = pd.DataFrame({"company": [company] * len(teams), "team": teams})
        df_list.append(df)
    team_table = pl.from_pandas(pd.concat(df_list))
    return team_table
