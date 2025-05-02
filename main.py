import argparse
import datetime as dt
import os
import pytz

from src.tracker.run_manager import RunManager
from src.uploader.run_uploader import RunUploader
from src.utils.config import CONFIG
from src.calculator.remove_tags import remove_latest_tags
from src.calculator.gpu_usage_calculator import GPUUsageCalculator

def validate_dates(start_date, end_date):
    # 今日の日付を取得
    LOCAL_TZ = pytz.timezone("Asia/Tokyo")
    default_start = dt.date(2024, 10, 25)
    default_end = dt.datetime.now(LOCAL_TZ).date() + dt.timedelta(days=-1)

    if start_date is None and end_date is None:
        # 両方指定なしの場合、昨日の日付を両方に設定
        return default_end.strftime("%Y-%m-%d"), default_end.strftime("%Y-%m-%d")
    elif start_date is None:
        # start_dateが指定されていない場合、デフォルトを2024-10-25に設定
        start = default_start
        end = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
    elif end_date is None:
        # end_dateが指定されていない場合、昨日の日付を設定
        start = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
        end = default_end
    else:
        # 両方指定ありの場合値をそのまま使用
        start = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
        end = dt.datetime.strptime(end_date, "%Y-%m-%d").date()

    # 日付の妥当性をチェック
    if start > end:
        raise ValueError("Start date must be before or equal to end date.")
    
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

def main():
    # 現在の日時（日本時間）
    current_time = dt.datetime.now(pytz.timezone('Asia/Tokyo'))
    print(f"Current time (JST): {current_time}")

    parser = argparse.ArgumentParser(description="Fetch and process run data from Weights & Biases")
    parser.add_argument("--api", type=str, help="Weights & Biases API Key")
    parser.add_argument("--start-date", type=str, help="Start date for data fetch (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date for data fetch (YYYY-MM-DD)")
    args = parser.parse_args()

    # API キーの処理
    if args.api is not None:
        if "WANDB_API_KEY" in os.environ:
            del os.environ["WANDB_API_KEY"]
        os.environ["WANDB_API_KEY"] = args.api
    elif "WANDB_API_KEY" not in os.environ:
        print("Warning: Weights & Biases API Key not provided. Some features may not work.")

    # 日付の検証
    start_date, end_date = validate_dates(args.start_date, args.end_date)
    date_range = [start_date, end_date]

    # 他の環境変数の設定
    os.environ["WANDB_CACHE_DIR"] = os.environ["WANDB_DATA_DIR"] = os.environ["WANDB_DIR"] = CONFIG.get('wandb_dir', '/tmp')

    print(f"Fetching data from {start_date} to {end_date}")

    # RunManagerの初期化と実行
    run_manager = RunManager(date_range)
    new_runs_df = run_manager.fetch_runs()

    # RunUploaderを使用してデータを処理しアップロード
    uploader = RunUploader(new_runs_df, date_range)
    processed_df = uploader.process_and_upload_runs()

    # latestタグを削除
    remove_latest_tags(date_range)

    # テーブルをアップデート
    calculator = GPUUsageCalculator(processed_df, date_range)
    calculator.update_tables()

if __name__ == "__main__":
    main()