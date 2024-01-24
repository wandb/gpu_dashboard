from get_runs import get_new_runs, update_data_src
from update_table import (
    remove_latest_tags,
    update_companies_table,
    update_overall_table,
)


def handler(event, context):
    new_runs_df = get_new_runs()
    all_runs_df = update_data_src(new_runs_df)
    remove_latest_tags()
    update_companies_table(all_runs_df)
    update_overall_table(all_runs_df)
    return


if __name__ == "__main__":
    handler(event=None, context=None)
