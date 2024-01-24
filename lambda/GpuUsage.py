import datetime

from func import (
    get_new_runs,
    update_data_src,
    remove_latest_tags,
    update_companies_table,
    update_overall_table,
)


def handler(event, context):
    tgt_date = None  # 今日を取得する場合はNone
    # tgt_date = datetime.datetime(2024, 1, 9)
    new_runs_df = get_new_runs()
    all_runs_df = update_data_src(new_runs_df)
    remove_latest_tags()
    update_companies_table(all_runs_df)
    update_overall_table(all_runs_df)
    return


if __name__ == "__main__":
    handler(event=None, context=None)
