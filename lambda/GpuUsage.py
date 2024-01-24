from get_new_runs import get_new_runs
from update_table import (
    collect_gpu_usage,
    remove_latest_tags,
    update_companies_table,
    update_overall_table,
)


def handler(event, context):
    # 最新データ取得
    # new_runs = get_new_runs()
    # 昨日時点の最新データ取得（なければskip）
    # old_runs = get_old_runs()
    # 最新データをconcat
    # latest_runs = join(new_runs, old_runs)
    # latestタグ削除
    # remove_latest_tags()
    # データ加工・テーブル出力
    # wandb.log()
    # 最新データをupdate
    # update_latest()
    get_new_runs()
    remove_latest_tags()
    latest_data_df = collect_gpu_usage()
    update_companies_table(latest_data_df)
    update_overall_table(latest_data_df)
    return


if __name__ == "__main__":
    handler(event=None, context=None)
