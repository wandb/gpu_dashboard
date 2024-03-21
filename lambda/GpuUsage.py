import os
import argparse
import datetime as dt
import json

import wandb

from config import CONFIG
from fetch_new_runs import fetch_new_runs
from utils import remove_project_tags
from handle_artifacts import handle_artifacts
from update_tables import update_tables


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
        target_date = dt.date.today() + dt.timedelta(days=-1)
    else:
        try:
            target_date = dt.datetime.strptime(target_date_str, "%Y-%m-%d").date()
        except:
            print(body := "!!! Invalid date format !!!")
            return {"statusCode": 200, "body": json.dumps(body)}

    # Check
    print(f"Test mode: {CONFIG.testmode}")
    print(f"Default entity: {wandb.api.default_entity}")
    print(f"Target date: {target_date}")

    # -------------------- データ更新 -------------------- #
    # Get new runs
    new_runs_df = fetch_new_runs(target_date=target_date)

    # Update artifacts
    all_runs_df = handle_artifacts(new_df=new_runs_df)

    # -------------------- テーブル更新 -------------------- #
    # Remove project tags
    remove_project_tags(
        entity=CONFIG.dashboard_path.entity,
        project=CONFIG.dashboard_path.project,
        delete_tags=CONFIG.dashboard_path.tag,
        num=(len(CONFIG.companies) + 2) * 2,  # +2はupdateとreadの分
    )
    # Update tables
    update_tables(all_runs_df=all_runs_df, target_date=target_date)
    return None


if __name__ == "__main__":
    # Parse
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", type=str, required=True)
    parser.add_argument("--target-date", type=str)
    args = parser.parse_args()
    # Run
    event = {"WANDB_API_KEY": args.api, "target_date": args.target_date}
    handler(event=event, context=None)
