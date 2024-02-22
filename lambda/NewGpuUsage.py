import os
import sys
import datetime as dt
import json

import polars as pl

from agg_df import agg_company_daily
from get_compy_runs import get_company_runs_df, update_artifacts
from utils import CONFIG


def handler(event: dict[str, str], context: object) -> None:
    ### set WANDB API KEY
    WANDB_API_KEY = event.get("WANDB_API_KEY")
    if WANDB_API_KEY is not None:
        del os.environ["WANDB_API_KEY"]
        os.environ["WANDB_API_KEY"] = WANDB_API_KEY
    os.environ["WANDB_CACHE_DIR"] = "/tmp"
    os.environ["WANDB_DATA_DIR"] = "/tmp"
    os.environ["WANDB_DIR"] = "/tmp"

    ### set target date
    target_date_str = event.get("target_date")
    if target_date_str is None:
        target_date = dt.date.today()
    else:
        try:
            target_date = dt.datetime.strptime(target_date_str, "%Y-%m-%d").date()
        except:
            return {"statusCode": 200, "body": json.dumps("Invalid date format.")}
    print(f"Processing {target_date} ...")

    ### create company table
    df_list = []
    for company in CONFIG["companies"]:
        company_runs_df = get_company_runs_df(
            company_name=company["company_name"], target_date=target_date, config=CONFIG
        )
        if company_runs_df.is_empty():
            continue
        df_list.append(company_runs_df)
    if not df_list:
        print("No runs detected.")
        return pl.DataFrame()
    new_df = pl.concat(df_list)
    all_runs_df = update_artifacts(df=new_df, target_date=target_date, config=CONFIG)
    return all_runs_df


if __name__ == "__main__":
    if len(sys.argv) == 2:
        event = {"WANDB_API_KEY": sys.argv[1]}
    elif len(sys.argv) == 3:
        event = {"WANDB_API_KEY": sys.argv[1], "target_date": sys.argv[2]}
    else:
        print("WANDB API KEY not provided.")
        exit()
    handler(event=event, context=None)
