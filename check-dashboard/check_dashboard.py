from dataclasses import dataclass
import datetime as dt
import pytz

from easydict import EasyDict
import wandb
import yaml


@dataclass
class CompanySchedule:
    company: str
    start_date: dt.datetime


@dataclass
class UpdateError:
    title: str
    text: str


with open("config.yaml", "r") as f:
    CONFIG = EasyDict(yaml.safe_load(f))

# 今日の日付を取得
LOCAL_TZ = pytz.timezone("Asia/Tokyo")
TARGET_DATE = dt.datetime.now(LOCAL_TZ).date() + dt.timedelta(days=-1)
TARGET_DATE_STR = TARGET_DATE.strftime("%Y-%m-%d")


def check_dashboard() -> None:
    """run_nameとtagをチェックする"""
    # 企業名のsetを取得
    company_schedule = get_company_schedule()
    companies = in_progress(company_schedule=company_schedule)
    companies.add("overall")
    # runを取得
    runs = get_runs()
    # チェック
    check(companies=companies, runs=runs)

    return None


def get_company_schedule() -> list[CompanySchedule]:
    """企業名と開始日を取得する"""
    companies = []
    for company in CONFIG.companies:
        start_date = min(
            [dt.datetime.strptime(s.date, "%Y-%m-%d").date() for s in company.schedule]
        )
        company_schedule = CompanySchedule(
            company=company.company, start_date=start_date
        )
        companies.append(company_schedule)

    return companies


def in_progress(company_schedule: list[CompanySchedule]) -> set[str]:
    """進行中の企業を取得する"""
    companies = set(
        c.company for c in company_schedule if TARGET_DATE >= c.start_date
    )

    return companies


def get_runs() -> object:
    """runを取得する"""
    api = wandb.Api()
    project_path = "/".join((CONFIG.dashboard.entity, CONFIG.dashboard.project))
    runs = api.runs(path=project_path)

    return runs


def check(companies: set[str], runs: object) -> None:
    # エラーを種週
    update_errors = []
    companies_found = []
    tag_for_latest = CONFIG.dashboard.tag_for_latest
    for run in runs:
        if tag_for_latest in run.tags:
            another_tags = [r for r in run.tags if r != tag_for_latest]
            # latest以外のtagの数が1つであることを確認
            try:
                assert len(another_tags) == 1
                companies_found.append(another_tags[0])
            except AssertionError:
                update_errors.append(
                    UpdateError(title="Error of number of tags", text=str(run.tags))
                )
            # 最新のデータが昨日分であることを確認
            try:
                target_date_str_found = run.name.split("_")[-1]
                assert target_date_str_found == TARGET_DATE_STR
            except AssertionError:
                update_errors.append(
                    UpdateError(
                        title="Error of target date",
                        text=f"{TARGET_DATE_STR}, {run.name}",
                    )
                )
    # 企業数の一致を確認
    try:
        assert len(companies) == len(companies_found)
    except AssertionError:
        update_errors.append(
            UpdateError(title="Error of number of latest runs", text="")
        )
    # 企業名の一致を確認
    try:
        assert companies == set(companies_found)
    except AssertionError:
        update_errors.append(
            UpdateError(title="Error of companies", text=str(companies_found))
        )
    # wandbでアラートを送信
    with wandb.init(
        entity=CONFIG.dashboard.entity,
        project=CONFIG.dashboard.project,
        name=f"Health Alert",
    ) as run:
        alert_title = "Dashboard health check"
        if update_errors:
            msg = ""
            for update_error in update_errors:
                msg += f"{update_error.title}: {update_error.text}\n"
            print(msg)
            wandb.alert(title=alert_title, text=msg)
        else:
            msg = "No errors found"
            print(msg)
            wandb.alert(title=alert_title, text=msg)

    return


if __name__ == "__main__":
    check_dashboard()
