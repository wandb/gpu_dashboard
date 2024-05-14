import datetime as dt

from easydict import EasyDict
import pandas as pd
import polars as pl

from config import CONFIG


class BlankTable:
    def __init__(self, target_date: dt.date = dt.date.today()):
        self.target_date = target_date
        self.team_table = team_table()
        self.daily_table = daily_table(target_date=target_date)
        self.monthy_table = monthly_table(daily_table=self.daily_table)
        self.overall_table = overall_table(daily_table=self.daily_table)


def daily_table(target_date: dt.date) -> pl.DataFrame:
    """スケジュールから日次テーブルを作成"""
    df_list = []
    schedules = get_schedule()
    for s in schedules:
        # 主要な日付だけのdf
        minimum_schedule_df = pl.DataFrame(s.schedule).with_columns(
            pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d").cast(pl.Date),
            pl.col("assigned_gpu_node").cast(pl.Float64),
        )
        # 日付を拡張
        date_df = pl.DataFrame(
            pl.datetime_range(
                start=min(minimum_schedule_df["date"]),
                end=target_date,
                interval="1d",
                eager=True,
            )
            .cast(pl.Date)
            .alias("date")
        )
        # 全期間にしてgpu_nodeの欠損を埋める
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
                # 提供終了した日付は削除
                pl.col("assigned_gpu_node")>0
            )
        )
        df_list.append(company_schedule_df)
    schedule_df = pl.concat(df_list)
    return schedule_df


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
