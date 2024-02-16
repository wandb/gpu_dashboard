# import os
import datetime

from func import (
    get_new_runs,
    update_artifacts,
    remove_latest_tags,
    update_companies_table,
    update_overall_table,
)
from utils import set_date


def handler(event, context):
    # TODO
    # - ステージング環境作成
    # - 簡易テスト（コード内に追加）
    # - 定期実行と手動データ取得
    # - データがない日に対応できているか
    # - リファクタ（日付をdatetime.datetimeに統合）
    if True:
        target_date = None
        target_date, processed_at = set_date(target_date=target_date)
        new_runs_df = get_new_runs(target_date=target_date, processed_at=processed_at)
        all_runs_df = update_artifacts(df=new_runs_df, target_date=target_date)
        remove_latest_tags()
        companies_daily_df = update_companies_table(
            df=all_runs_df, target_date=target_date
        )
        update_overall_table(df=companies_daily_df, target_date=target_date)
    return


if __name__ == "__main__":
    handler(event=None, context=None)
