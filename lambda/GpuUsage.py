"""
TODO
- ステージング環境作成
- 簡易テスト（コード内に追加）
- データがない日に対応できているか
"""

import os
import datetime as dt
import json
import sys

from func import (
    get_new_runs,
    update_artifacts,
    remove_latest_tags,
    update_companies_table,
    update_overall_table,
)

def handler(event: dict[str, str], context: object) -> None:
    ### set WANDB API KEY
    WANDB_API_KEY = event.get("WANDB_API_KEY")
    if WANDB_API_KEY is not None:
        del os.environ["WANDB_API_KEY"]
        os.environ["WANDB_API_KEY"] = WANDB_API_KEY
    ### set target date
    target_date_str = event.get("target_date")
    if target_date_str is None:
        target_date = dt.date.today()
    else:
        try:
            target_date = dt.datetime.strptime(target_date_str, "%Y-%m-%d").date()
        except:
            return {"statusCode": 200, "body": json.dumps("Invalid date format.")}
    print(f"Processing {target_date}")
    ### update tables
    new_runs_df = get_new_runs(target_date=target_date)
    all_runs_df = update_artifacts(df=new_runs_df, target_date=target_date)
    # remove_latest_tags()
    companies_daily_df = update_companies_table(df=all_runs_df, target_date=target_date)
    update_overall_table(df=companies_daily_df, target_date=target_date)
    return {"statusCode": 200, "body": json.dumps("Succeeded.")}


if __name__ == "__main__":
    if len(sys.argv)==2:
        event = {"WANDB_API_KEY": sys.argv[1]}
    elif len(sys.argv)==3:
        event = {"WANDB_API_KEY": sys.argv[1], "target_date": sys.argv[2]}
    else:
        print("WANDB API KEY not provided.")
        exit()
    handler(event=event, context=None)
