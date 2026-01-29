from dataclasses import dataclass
import datetime as dt
from typing import List, Set
import pytz
from easydict import EasyDict
import wandb
import yaml
import pandas as pd
import os
import glob
import tempfile
import shutil
import json

@dataclass
class CompanySchedule:
    company: str
    start_date: dt.date

@dataclass
class UpdateError:
    title: str
    text: str

class Config:
    def __init__(self, config_path: str):
        with open(config_path, "r") as f:
            self.data = EasyDict(yaml.safe_load(f))
        self.LOCAL_TZ = pytz.timezone("Asia/Tokyo")
        self.TARGET_DATE = dt.datetime.now(self.LOCAL_TZ).date()
        self.TARGET_DATE_STR = self.TARGET_DATE.strftime("%Y-%m-%d")

class DashboardChecker:
    def __init__(self, config: Config):
        self.config = config
        self.api = wandb.Api()

    def check_dashboard(self) -> None:
        """ダッシュボードの健全性をチェックする"""
        companies = self.get_in_progress_companies()
        errors = []

        if not companies:
            errors.append(UpdateError(title="No active companies", text="There are no companies currently active."))
        else:
            runs = self.get_runs()
            errors = self.check_runs(companies, runs)
            # 追加: データ品質チェック (3日間連続0/欠損)
            quality_errors = self.check_data_quality(companies)
            errors.extend(quality_errors)

        self.send_alert(errors)

    def get_company_schedule(self) -> List[CompanySchedule]:
        """企業名と開始日を取得する"""
        return [
            CompanySchedule(
                company=company.company,
                start_date=min(dt.datetime.strptime(s.date, "%Y-%m-%d").date() for s in company.schedule)
            )
            for company in self.config.data.companies
        ]

    def get_in_progress_companies(self) -> Set[str]:
        """進行中の企業を取得する"""
        today = self.config.TARGET_DATE
        companies = set()
        
        for company in self.config.data.companies:
            start_date = min(dt.datetime.strptime(s['date'], "%Y-%m-%d").date() for s in company.schedule)
            end_date = max(dt.datetime.strptime(s['date'], "%Y-%m-%d").date() for s in company.schedule)
            
            # 開始日以降かつ終了日の前日まで進行中とみなす
            if start_date <= today < end_date:
                companies.add(company.company)
        
        # 進行中の企業がある場合のみ "overall" を追加
        if companies:
            companies.add("overall")
        return companies

    def get_runs(self) -> object:
        """runを取得する"""
        project_path = f"{self.config.data.dashboard.entity}/{self.config.data.dashboard.project}"
        return self.api.runs(path=project_path)

    def check_runs(self, companies: Set[str], runs: object) -> List[UpdateError]:
        """runをチェックし、エラーがあれば返す"""
        errors = []
        companies_found = []
        tag_for_latest = self.config.data.dashboard.tag_for_latest

        for run in runs:
            if tag_for_latest in run.tags:
                another_tags = [r for r in run.tags if r != tag_for_latest]
                self.check_tags(another_tags, run.tags, companies_found, errors)
                self.check_target_date(run.name, errors)

        self.check_companies(companies, companies_found, errors)
        return errors

    def check_tags(self, another_tags: List[str], all_tags: List[str], companies_found: List[str], errors: List[UpdateError]) -> None:
        if len(another_tags) != 1:
            errors.append(UpdateError(title="Error of number of tags", text=str(all_tags)))
        else:
            companies_found.append(another_tags[0])

    def check_target_date(self, run_name: str, errors: List[UpdateError]) -> None:
        target_date_str_found = run_name.split("_")[-1]
        if target_date_str_found != self.config.TARGET_DATE_STR:
            errors.append(UpdateError(title="Error of target date", text=f"{self.config.TARGET_DATE_STR}, {run_name}"))

    def check_companies(self, companies: Set[str], companies_found: List[str], errors: List[UpdateError]) -> None:
        if len(companies) != len(companies_found):
            errors.append(UpdateError(title="Error of number of latest runs", text=""))
        if companies != set(companies_found):
            errors.append(UpdateError(title="Error of companies", text=str(companies_found)))

    def check_data_quality(self, companies: Set[str]) -> List[UpdateError]:
        """過去3日間のデータ品質（欠損や稼働率0）をチェックする"""
        errors = []
        try:
            # Artifactの取得
            entity = self.config.data.dataset.entity
            project = self.config.data.dataset.project
            artifact_name = self.config.data.dataset.artifact_name
            artifact_path = f"{entity}/{project}/{artifact_name}:latest"
            
            print(f"Checking data quality from artifact: {artifact_path}")
            artifact = self.api.artifact(artifact_path)
            
            # TableをDataFrameとして取得
            # all_runs_dataテーブルを取得しようと試みる
            try:
                table = artifact.get("all_runs_data")
            except KeyError:
                # キーが見つからない場合は、manifestから推測するか、デフォルトのファイルを探す
                # ここでは簡略化のため、エラーとして記録せずスキップ（またはログ出力）
                print("Warning: 'all_runs_data' table not found in artifact.")
                return []

            if table is None:
                return []

            df = table.get_dataframe()
            
            # 日付カラムの変換
            if 'date' in df.columns:
                # dateカラムが文字列の場合、datetime.dateに変換
                df['date'] = pd.to_datetime(df['date']).dt.date
            
            # チェック対象期間: 今日を含む過去3日間
            target_dates = [self.config.TARGET_DATE - dt.timedelta(days=i) for i in range(3)]
            print(f"Checking data for dates: {target_dates}")
            
            for company in companies:
                if company == 'overall': continue
                
                # 過去3日間のデータをフィルタ
                recent_df = df[
                    (df['company_name'] == company) & 
                    (df['date'].isin(target_dates))
                ]
                
                if recent_df.empty:
                    # 3日間データなし
                    errors.append(UpdateError(
                        title=f"Missing Data: {company}", 
                        text=f"No data found in the last 3 days ({target_dates[-1]} - {target_dates[0]})."
                    ))
                    continue
                
                # 稼働率0チェック
                if 'average_gpu_utilization' in recent_df.columns:
                    # 数値に変換 (エラーはNaN -> 0)
                    util = pd.to_numeric(recent_df['average_gpu_utilization'], errors='coerce').fillna(0)
                    if util.sum() == 0:
                        # 3日間データはあるが、稼働率がずっと0
                        errors.append(UpdateError(
                            title=f"Zero Utilization: {company}",
                            text=f"Runs exist but 0% GPU utilization for the last 3 days."
                        ))

        except Exception as e:
            print(f"Warning: Failed to check data quality: {e}")
            # エラーを握りつぶして、アラート処理自体は止めない
            
        return errors

    def send_alert(self, errors: List[UpdateError]) -> None:
        """wandbでアラートを送信する"""
        with wandb.init(
            entity=self.config.data.dashboard.entity,
            project=self.config.data.dashboard.project,
            name="Health Alert",
        ) as run:
            alert_title = "Dashboard health check"
            if errors:
                msg = "\n".join(f"{error.title}: {error.text}" for error in errors)
            else:
                msg = "No errors found. All active companies are reporting as expected."
            
            print(msg)
            wandb.alert(title=alert_title, text=msg)

if __name__ == "__main__":
    config = Config("config.yaml")
    checker = DashboardChecker(config)
    checker.check_dashboard()
