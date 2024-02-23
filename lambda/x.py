import os
import sys
import datetime as dt
import json

from easydict import EasyDict
import polars as pl
from tqdm import tqdm
import wandb
import yaml

from y import pipeline, update_artifacts


def handler(event: dict[str, str], context: object) -> None:
    ### Read yaml
    with open("config.yaml") as y:
        config = EasyDict(yaml.safe_load(y))

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
    print(f"Processing {target_date} ...")

    ### Get new runs
    df_list = []
    company_config: EasyDict
    for company_config in tqdm(config.companies):
        company_runs_df: pl.DataFrame = pipeline(
            company_name=company_config.company_name,
            gpu_schedule=company_config.schedule,
            target_date=target_date,
            logged_at=dt.datetime.now(),
        )
        if company_runs_df.is_empty():
            continue
        else:
            df_list.append(company_runs_df)
    if df_list:
        new_runs_df = pl.concat(df_list)
    else:
        print(body := "!!! No runs found !!!")
        return {"statusCode": 200, "body": body}

    ### Update artifacts
    result: dict = update_artifacts(new_runs_df=new_runs_df)

    return {"statusCode": 200, "body": json.dumps(result)}


if __name__ == "__main__":
    if len(sys.argv) == 2:
        event = {"WANDB_API_KEY": sys.argv[1]}
    elif len(sys.argv) == 3:
        event = {"WANDB_API_KEY": sys.argv[1], "target_date": sys.argv[2]}
    else:
        print("!!! Invalid numbers of args !!!")
        exit()
    handler(event=event, context=None)
