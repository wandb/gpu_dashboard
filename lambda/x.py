import os
import sys
import argparse
import datetime as dt
import json
import time

from easydict import EasyDict
import polars as pl
from tqdm import tqdm
import wandb
import yaml

from y import pipeline, update_artifacts


def handler(event: dict[str, str], context: object) -> None:
    start_time = time.time()
    ### Read yaml
    with open("config.yaml") as y:
        config = EasyDict(yaml.safe_load(y))

    ### Test mode
    print(f"Test mode: {config.testmode}")

    ### Set WANDB envirionment
    WANDB_API_KEY = event.get("WANDB_API_KEY")
    if WANDB_API_KEY is not None:
        del os.environ["WANDB_API_KEY"]
        os.environ["WANDB_API_KEY"] = WANDB_API_KEY
    environ = config.environ
    os.environ["WANDB_CACHE_DIR"] = environ.WANDB_CACHE_DIR
    os.environ["WANDB_DATA_DIR"] = environ.WANDB_DATA_DIR
    os.environ["WANDB_DIR"] = environ.WANDB_DIR
    # Check
    print(f"Default entity: {wandb.api.default_entity}")

    ### Set target date
    target_date: dt.date
    target_date_str = event.get("target_date")
    if target_date_str is None:
        target_date = dt.date.today()
    else:
        try:
            target_date = dt.datetime.strptime(target_date_str, "%Y-%m-%d").date()
        except:
            print(body := "!!! Invalid date format !!!")
            return {"statusCode": 200, "body": json.dumps(body)}
    # Check
    print(f"Target date: {target_date}")

    ### Get new runs
    df_list = []
    company_config: EasyDict
    for company_config in tqdm(config.companies):
        if (config.testmode) & (len(df_list) == 2):
            continue
        company_runs_df: pl.DataFrame = pipeline(
            company_name=company_config.company_name,
            gpu_schedule=company_config.schedule,
            target_date=target_date,
            logged_at=dt.datetime.now(),
            ignore_tag=config.ignore_tag,
            testmode=config.testmode,
        )
        if company_runs_df.is_empty():
            continue
        df_list.append(company_runs_df)
    if not df_list:
        print("No runs found")
        new_runs_df = pl.DataFrame()
    else:
        new_runs_df = pl.concat(df_list)
        print(f"{len(new_runs_df)} runs found.")

    ### Update artifacts
    end_time = time.time()
    elapsed_time = "{} min {} sec".format(
        int((end_time - start_time) // 60), int((end_time - start_time) % 60)
    )
    result: dict = update_artifacts(
        new_runs_df=new_runs_df,
        target_date=target_date,
        path_to_dashboard=config.path_to_dashboard,
        elapsed_time=elapsed_time,
        testmode=config.testmode,
    )
    ### Add summary
    result["elapsed_time"] = elapsed_time
    result["target_date"] = target_date_str
    result["testmode"] = config.testmode
    print(result)
    return {"statusCode": 200, "body": json.dumps(result)}


if __name__ == "__main__":
    ### Parse
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", type=str, required=True)  # 「--」無しだと必須の引数
    parser.add_argument("--target-date", type=str)  # 「--」付きだとオプション引数
    args = parser.parse_args()
    ### Run
    event = {"WANDB_API_KEY": args.api, "target_date": args.target_date}
    handler(event=event, context=None)
