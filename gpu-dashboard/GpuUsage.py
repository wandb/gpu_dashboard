import os
import argparse
import datetime as dt
import functools
import pytz
import traceback

import wandb

from config import CONFIG
from fetch_runs import fetch_runs
from handle_artifacts import handle_artifacts
from remove_tags import remove_tags
from update_tables import update_tables


def wandb_alert(alert_title: str, error_message: str) -> None:
    with wandb.init(
        entity=CONFIG.dashboard.entity,
        project=CONFIG.dashboard.project,
        name=alert_title,
    ) as run:
        wandb.alert(title=alert_title, text=error_message)

    return None


def error_handler(func: callable) -> callable:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_message = traceback.format_exc()
            print(error_message)
            wandb_alert(alert_title="Runtime Error", error_message=error_message)
            raise

    return wrapper

@error_handler
def handler(event: dict[str, str], context: object) -> None:
    # -------------------- 準備 -------------------- #
    # Set WANDB envirionment
    WANDB_API_KEY = event.get("WANDB_API_KEY")
    if WANDB_API_KEY is not None:
        del os.environ["WANDB_API_KEY"]
        os.environ["WANDB_API_KEY"] = WANDB_API_KEY
    os.environ["WANDB_CACHE_DIR"] = CONFIG.wandb_dir
    os.environ["WANDB_DATA_DIR"] = CONFIG.wandb_dir
    os.environ["WANDB_DIR"] = CONFIG.wandb_dir

    # Set target date
    target_date: dt.date
    target_date_str = event.get("target_date")
    if target_date_str is None:
        local_tz = pytz.timezone("Asia/Tokyo")
        target_date = dt.datetime.now(local_tz).date() + dt.timedelta(days=-1)
    else:
        try:
            target_date = dt.datetime.strptime(target_date_str, "%Y-%m-%d").date()
        except:
            print("!!! Invalid date format !!!")

    # Check
    print(f"Test mode: {CONFIG.testmode}")
    print(f"Enable alert: {CONFIG.enable_alert}")
    print(f"Default entity: {wandb.api.default_entity}")
    print(f"Target date: {target_date}")

    # -------------------- データ更新 -------------------- #
    # Get new runs
    new_runs_df = fetch_runs(target_date=target_date)

    # Update artifacts
    all_runs_df = handle_artifacts(new_runs_df=new_runs_df, target_date=target_date)

    # -------------------- テーブル更新 -------------------- #
    # Remove project tags
    remove_tags()
    # Update tables
    update_tables(all_runs_df=all_runs_df, target_date=target_date)

    return None


if __name__ == "__main__":
    # Parse
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", type=str)
    parser.add_argument("--target-date", type=str)
    args = parser.parse_args()
    # Run
    event = {"WANDB_API_KEY": args.api, "target_date": args.target_date}
    handler(event=event, context=None)
