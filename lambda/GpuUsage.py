from get_new_runs import get_new_runs
from update_table import (
    collect_gpu_usage,
    remove_latest_tags,
    update_companies_table,
    update_overall_table,
)


def handler(event, context):
    get_new_runs()
    remove_latest_tags()
    latest_data_df = collect_gpu_usage()
    update_companies_table(latest_data_df)
    update_overall_table(latest_data_df)
    return


if __name__ == "__main__":
    handler(event=None, context=None)
