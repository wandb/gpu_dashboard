import datetime as dt
import pandas as pd
import polars as pl
from easydict import EasyDict
from src.utils.config import CONFIG

class BlankTable:
    def __init__(self, target_date: dt.date = dt.date.today()):
        self.target_date = target_date
        self.__team_table()
        self.__daily_table()
        self.__weekly_table()
        self.__monthly_table()
        self.__overall_table()
    
    def __team_table(self) -> pl.DataFrame:
        """企業とチームの対応テーブルを作成"""
        df_list = []
        for c in CONFIG.companies:
            company = c["company"]
            teams = c["teams"]
            df = pd.DataFrame({"company": [company] * len(teams), "team": teams})
            df_list.append(df)
        self.team_table = pl.from_pandas(pd.concat(df_list))
    
    def __get_schedule(self) -> list[EasyDict]:
        """configからスケジュールだけ取り出す"""
        comps = CONFIG.companies
        schedules = []
        keys = {"company", "schedule"}
        for comp in comps:
            s = EasyDict({k: v for k, v in comp.items() if k in keys})
            schedules.append(s)
        return schedules

    def __daily_table(self) -> pl.DataFrame:
        df_list = []
        schedules = self.__get_schedule()
        for s in schedules:
            minimum_schedule_df = pl.DataFrame(s.schedule).with_columns(
                pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"),
                pl.col("assigned_gpu_node").cast(pl.Float64),
            )
            
            # 日付範囲の開始日と終了日を決定
            start_date = min(minimum_schedule_df["date"].min(), self.target_date)
            end_date = min(minimum_schedule_df["date"].max(), self.target_date)
            
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
        self.daily_table = schedule_df

    def __weekly_table(self) -> pl.DataFrame:
        """日次テーブルから週次テーブルを作成"""
        # target_date の週の開始日（月曜日）を計算
        target_week_start = self.target_date - dt.timedelta(days=(self.target_date.weekday() + 1) % 7)
        
        # 前の週の土曜日を計算
        last_complete_week_end = target_week_start + dt.timedelta(days=6)
        
        self.weekly_table = (
            self.daily_table
            .filter(pl.col("date") <= last_complete_week_end)
            .with_columns(
                (pl.col("date") - pl.duration(days=(pl.col("date").dt.weekday()) % 7)).alias("week_start")
            )
            .group_by("company", "week_start")
            .agg(pl.col("assigned_gpu_node").sum())
            .sort("week_start", "company")
            .select(
                pl.col("company").cast(pl.Utf8),
                pl.col("week_start").cast(pl.Date),
                pl.col("assigned_gpu_node").cast(pl.Int64),
            )
        )

    def __monthly_table(self) -> pl.DataFrame:
        """日次テーブルから月次テーブルを作成"""
        first_day_of_current_month = self.target_date.replace(day=1)
        
        self.monthly_table = (
            self.daily_table
            .filter(pl.col("date") < first_day_of_current_month)
            .with_columns(
                pl.col("date").dt.strftime("%Y-%m").alias("year_month")
            )
            .group_by("company", "year_month")
            .agg(
                pl.col("assigned_gpu_node").sum().alias("assigned_gpu_node")
            )
            .sort("year_month", "company")
            .select(
                pl.col("company").cast(pl.Utf8),
                pl.col("year_month").cast(pl.Utf8),
                pl.col("assigned_gpu_node").cast(pl.Int64),
            )
        )

    def __overall_table(self) -> pl.DataFrame:
        """日次テーブルから全期間テーブルを作成"""
        self.overall_table = (
            self.daily_table.group_by("company")
            .agg(pl.col("assigned_gpu_node").sum())
            .sort("company")
            .select(
                pl.col("company").cast(pl.Utf8),
                pl.col("assigned_gpu_node").cast(pl.Int64),
            )
        )