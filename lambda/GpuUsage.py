import os
import datetime
import json

from func import (
    get_new_runs,
    update_artifacts,
    remove_latest_tags,
    update_companies_table,
    update_overall_table,
)
from utils import NOW_UTC


def handler(event: dict[str, str], context: object) -> None:
    # TODO
    # - ステージング環境作成
    # - 簡易テスト（コード内に追加）
    # - データがない日に対応できているか
    # - リファクタ（日付をdatetime.datetimeに統合）
    target_date_str = event.get("target_date")
    if target_date_str is None:
        target_date = datetime.date.today()
        processed_at = NOW_UTC + datetime.timedelta(hours=9)
    else:
        try:
            target_date = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date()
            processed_at = datetime.datetime.combine(
                target_date + datetime.timedelta(days=1), datetime.time()
            )
        except:
            return {"statusCode": 200, "body": json.dumps("Invalid date format.")}
    # target_date, processed_at = set_date(target_date=target_date)
    print(f"Processing {target_date}")
    new_runs_df = get_new_runs(target_date=target_date, processed_at=processed_at)
    all_runs_df = update_artifacts(df=new_runs_df, target_date=target_date)
    remove_latest_tags()
    companies_daily_df = update_companies_table(df=all_runs_df, target_date=target_date)
    update_overall_table(df=companies_daily_df, target_date=target_date)
    return {"statusCode": 200, "body": json.dumps("Succeeded.")}


if __name__ == "__main__":
    handler(event=None, context=None)
