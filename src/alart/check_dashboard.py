from dataclasses import dataclass
import datetime as dt
from typing import List, Set, Any
import pytz
from easydict import EasyDict
import wandb
import yaml
import polars as pl

@dataclass
class CompanySchedule:
    company: str
    start_date: dt.date
    end_date: dt.date

@dataclass
class UpdateError:
    title: str
    text: str

class Config:
    def __init__(self, config_path: str):
        with open(config_path, "r") as f:
            self.data = EasyDict(yaml.safe_load(f))
        self.LOCAL_TZ = pytz.timezone("Asia/Tokyo")
        self.TARGET_DATE = dt.datetime.now(self.LOCAL_TZ).date() + dt.timedelta(days=-1)
        self.TARGET_DATE_STR = self.TARGET_DATE.strftime("%Y-%m-%d")

class DashboardChecker:
    def __init__(self, config: Config):
        self.config = config
        self.api = wandb.Api()
        self.company_data = {
            comp.company: comp for comp in self.config.data.companies
        }

    def check_dashboard(self) -> None:
        """ダッシュボードの健全性をチェックする"""
        companies = self.get_in_progress_companies()
        errors = []

        if companies:
            runs = self.get_runs()
            errors.extend(self.check_runs(companies, runs))
            errors.extend(self.check_artifacts(companies, runs))
            errors.extend(self.check_deleted_runs())
        else:
            errors.append(UpdateError(title="No active companies", text="There are no companies currently active."))
        
        self.handle_errors(errors)

    def get_company_schedule(self) -> List[CompanySchedule]:
        """企業名と開始日、終了日を取得する"""
        return [
            CompanySchedule(
                company=company.company,
                start_date=self.parse_date(min(s.date for s in company.schedule)),
                end_date=self.parse_date(max(s.date for s in company.schedule))
            )
            for company in self.config.data.companies
        ]

    def get_in_progress_companies(self) -> Set[str]:
        """進行中の企業を取得する"""
        companies = set()
        for company, data in self.company_data.items():
            current_gpu = 0
            for schedule in data.schedule:
                schedule_date = dt.datetime.strptime(schedule.date, "%Y-%m-%d").date()
                if schedule_date <= self.config.TARGET_DATE:
                    current_gpu = schedule.assigned_gpu_node
            
            if current_gpu > 0:
                companies.add(company)
        
        if companies:
            companies.add("overall")
        return companies

    def get_runs(self) -> List[Any]:
        """runを取得する"""
        project_path = f"{self.config.data.dashboard.entity}/{self.config.data.dashboard.project}"
        return list(self.api.runs(path=project_path))

    def check_runs(self, companies: Set[str], runs: List[Any]) -> List[UpdateError]:
        """runをチェックし、エラーがあれば返す"""
        errors = []
        companies_found = set()
        tag_for_latest = self.config.data.dashboard.tag_for_latest
        
        for run in runs:
            if tag_for_latest in run.tags:
                company_tags = [r for r in run.tags if r != tag_for_latest]
                if len(company_tags) == 1 and company_tags[0] in companies:
                    companies_found.add(company_tags[0])
                    self.check_target_date(run.name, run.tags, errors)
        
        self.check_missing_companies(companies, companies_found, errors)
        self.check_extra_companies(companies, companies_found, errors)
        
        return errors

    def check_target_date(self, run_name: str, run_tags: List[str], errors: List[UpdateError]) -> None:
        target_date_str_found = run_name.split("_")[-1]
        target_date = dt.datetime.strptime(target_date_str_found, "%Y-%m-%d").date()
        company_tag = [tag for tag in run_tags if tag != self.config.data.dashboard.tag_for_latest][0]
        company_data = self.company_data.get(company_tag)

        if company_data:
            current_gpu = 0
            last_active_date = None
            
            for i, schedule in enumerate(company_data.schedule):
                schedule_date = dt.datetime.strptime(schedule.date, "%Y-%m-%d").date()
                
                if schedule_date <= self.config.TARGET_DATE:
                    current_gpu = schedule.assigned_gpu_node
                    
                    if current_gpu > 0 and i + 1 < len(company_data.schedule):
                        next_schedule = company_data.schedule[i + 1]
                        next_date = dt.datetime.strptime(next_schedule.date, "%Y-%m-%d").date()
                        if next_schedule.assigned_gpu_node == 0:
                            last_active_date = next_date - dt.timedelta(days=1)

            if current_gpu == 0 and last_active_date:
                if target_date != last_active_date:
                    errors.append(UpdateError(
                        title="Error of target date",
                        text=f"Company {company_tag}: Expected last active date: {last_active_date.strftime('%Y-%m-%d')}, Found: {target_date_str_found}"
                    ))
            else:
                if target_date_str_found != self.config.TARGET_DATE_STR:
                    errors.append(UpdateError(
                        title="Error of target date",
                        text=f"Company {company_tag}: Expected: {self.config.TARGET_DATE_STR}, Found: {target_date_str_found}"
                    ))

    def check_missing_companies(self, expected: Set[str], found: Set[str], errors: List[UpdateError]) -> None:
        missing = expected - found
        if missing:
            errors.append(UpdateError(title="Missing latest runs", text=f"Companies without latest runs: {missing}"))

    def check_extra_companies(self, expected: Set[str], found: Set[str], errors: List[UpdateError]) -> None:
        extra = found - expected
        if extra:
            errors.append(UpdateError(title="Unexpected latest runs", text=f"Unexpected companies with latest runs: {extra}"))

    def check_artifacts(self, companies: Set[str], runs: List[Any]) -> List[UpdateError]:
        """アーティファクトをチェックし、エラーがあれば返す"""
        errors = []
        tag_for_latest = self.config.data.dashboard.tag_for_latest

        for run in runs:
            if tag_for_latest in run.tags:
                company_tags = [r for r in run.tags if r != tag_for_latest]
                if len(company_tags) == 1 and company_tags[0] in companies and company_tags[0] != "overall":
                    company = company_tags[0]
                    errors.extend(self.check_company_artifact(company, run))

        return errors

    def check_company_artifact(self, company: str, run: Any) -> List[UpdateError]:
        errors = []
        company_data = self.company_data.get(company)

        if company_data:
            current_gpu = 0
            for schedule in company_data.schedule:
                schedule_date = dt.datetime.strptime(schedule.date, "%Y-%m-%d").date()
                if schedule_date <= self.config.TARGET_DATE:
                    current_gpu = schedule.assigned_gpu_node

            if current_gpu > 0:
                try:
                    artifact_name = f"run-{run.id}-company_daily_gpu_usage:v0"
                    artifact = self.api.artifact(f"{self.config.data.dashboard.entity}/{self.config.data.dashboard.project}/{artifact_name}")
                    
                    table = artifact.get('company_daily_gpu_usage')
                    df = table.get_dataframe()

                    if 'GPU稼働率(%)' in df.columns:
                        latest_gpu_usage = df['GPU稼働率(%)'].iloc[0]
                        if latest_gpu_usage <= 10:
                            errors.append(UpdateError(
                                title=f"Low GPU Usage for {company}",
                                text=f"Latest GPU usage is {latest_gpu_usage:.2f}%, which is below 10%"
                            ))
                except Exception as e:
                    errors.append(UpdateError(
                        title=f"Error checking artifacts for {company}",
                        text=str(e)
                    ))
        return errors
    
    def check_deleted_runs(self) -> List[UpdateError]:
        """削除が検知されたrunをチェックする"""
        try:
            artifact = self.api.artifact(f"{self.config.data.dashboard.entity}/{self.config.data.dashboard.project}/{self.config.data.dataset.deleted_runs_artifact_name}:latest")
            deleted_runs_df = pl.read_csv(artifact.file())
            
            if not deleted_runs_df.is_empty():
                # 会社ごとに削除されたランの数をカウント
                company_counts = deleted_runs_df.group_by('company_name').agg(pl.count('run_id').alias('count'))
                
                # 削除されたランの総数
                total_deleted = deleted_runs_df.shape[0]
                
                # 要約メッセージを作成
                summary = f"Total {total_deleted} runs deleted across {company_counts.shape[0]} companies.\n"
                for row in company_counts.iter_rows(named=True):
                    summary += f"- {row['company_name']}: {row['count']} run(s)\n"
                
                return [UpdateError(
                    title="Deleted Runs Summary",
                    text=summary
                )]
            else:
                print("No deleted runs detected.")
                return []
        except Exception as e:
            return [UpdateError(
                title="Error checking deleted runs",
                text=f"An error occurred while checking for deleted runs: {str(e)}"
            )]
    
    def handle_errors(self, errors: List[UpdateError]) -> None:
        if self.config.data.enable_alert:
            self.send_alert(errors)
        else:
            for error in errors:
                print(f"{error.title}: {error.text}")

    def send_alert(self, errors: List[UpdateError]) -> None:
        """wandbでアラートを送信する"""
        with wandb.init(
            entity=self.config.data.dashboard.entity,
            project=self.config.data.dashboard.project,
            name="Health Alert",
        ) as run:
            alert_title = f"Dashboard health check for {self.config.TARGET_DATE_STR}"
            msg = self.format_alert_message(errors)
            
            print(msg)
            wandb.alert(title=alert_title, text=msg)

    def format_alert_message(self, errors: List[UpdateError]) -> str:
        header = f"Target Date: {self.config.TARGET_DATE_STR}\n\n"
        if errors:
            return header + "\n".join(f"{error.title}: {error.text}" for error in errors)
        else:
            return header + "No errors found. All active companies are reporting as expected."

    @staticmethod
    def parse_date(date_str: str) -> dt.date:
        return dt.datetime.strptime(date_str, "%Y-%m-%d").date()

if __name__ == "__main__":
    config = Config("config.yaml")
    checker = DashboardChecker(config)
    checker.check_dashboard()