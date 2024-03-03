import os
import argparse
import datetime as dt
import json
import time

from easydict import EasyDict
import polars as pl
from tqdm import tqdm
import wandb
import yaml

from y import pipeline, update_artifacts, update_tables, remove_project_tags


def handler(event: dict[str, str], context: object) -> None:
    start_time = time.time()
    ### Read yaml
    with open("config.yaml") as y:
        config = EasyDict(yaml.safe_load(y))

    ### Test mode
    print(f"Test mode: {config.testmode}")

    ### Set WANDB envirionment
    if event.get("WANDB_API_KEY") is not None:
        os.environ["WANDB_API_KEY"] = event.get("WANDB_API_KEY")
    os.environ["WANDB_CACHE_DIR"] = config.wandb_dir
    os.environ["WANDB_DATA_DIR"] = config.wandb_dir
    os.environ["WANDB_DIR"] = config.wandb_dir
    # Check
    print(f"Default entity: {wandb.api.default_entity}")

    ### Set target date
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
    print(f"Target date: {target_date}")

    # -------------#
    # Update data #
    # -------------#
    ## Get new runs
    df_list = []
    company_config: EasyDict
    for company_config in tqdm(config.companies):
        if (config.testmode) & (len(df_list) == 2):
            continue
        company_runs_df: pl.DataFrame = pipeline(
            company_name=company_config.company_name,
            gpu_schedule=company_config.schedule,
            target_date=target_date,
            ignore_project=company_config.get("ignore_project"),
            # ignore_project=None, # for test
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

    ### Remove project tags
    remove_project_tags(
        entity=config.path_to_dashboard.entity,
        project=config.path_to_dashboard.project,
        delete_tags=config.tag_for_latest,
        head=(len(config.companies) + 2) * 2,  # +2はupdateとreadの分
    )
    ## Update artifacts
    end_time1 = time.time()
    elapsed_time1 = "{} min {} sec".format(
        int((end_time1 - start_time) // 60), int((end_time1 - start_time) % 60)
    )
    new_records: int = update_artifacts(
        new_runs_df=new_runs_df,
        target_date=target_date,
        wandb_dir=config.wandb_dir,
        path_to_dashboard=config.path_to_dashboard,
        elapsed_time=elapsed_time1,
        testmode=config.testmode,
    )
    ### Add summary
    result = {}
    result["target_date"] = target_date_str
    result["new_records"] = new_records
    result["elapsed_time1"] = elapsed_time1
    result["testmode"] = config.testmode
    # --------------#
    # Update table #
    # --------------#
    ### Upate tables
    update_tables(
        wandb_dir=config.wandb_dir,
        companies_config=config.companies,
        path_to_dashboard=config.path_to_dashboard,
        target_date=target_date,
        tag_for_latest=config.tag_for_latest,
    )
    return {"statusCode": 200, "body": json.dumps(result)}


if __name__ == "__main__":
    ### Parse
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", type=str)
    parser.add_argument("--target-date", type=str)
    args = parser.parse_args()
    ### Run
    event = {"WANDB_API_KEY": args.api, "target_date": args.target_date}
    handler(event=event, context=None)
