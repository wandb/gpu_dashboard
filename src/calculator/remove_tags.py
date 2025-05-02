import wandb
from typing import List
from src.utils.config import CONFIG
import datetime

def remove_latest_tags(date_range: List):
    # W&B APIの初期化
    api = wandb.Api()

    # CONFIGからentity、project、latest_tagを取得
    entity = CONFIG.dashboard.entity
    project = CONFIG.dashboard.project
    latest_tag = CONFIG.dashboard.tag_for_latest

    # 日付範囲の解析
    start_date = datetime.datetime.strptime(date_range[0], "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(date_range[1], "%Y-%m-%d").date()

    # CONFIGから会社名のリストとスケジュールを作成
    companies = CONFIG.companies

    # 'latest'タグを持つrunを取得
    runs = api.runs(f"{entity}/{project}", {"tags": {"$in": [latest_tag]}})

    removed_count = 0
    for run in runs:
        # runのタグを取得
        tags = run.tags
        
        # latest_tagと（会社名のいずれかまたは'overall'）がタグに含まれているか確認
        matching_companies = [company for company in companies if company['company'] in tags]
        
        should_remove_tag = False
        
        if latest_tag in tags and (matching_companies or 'overall' in tags):
            if 'overall' in tags:
                should_remove_tag = True
            else:
                for company in matching_companies:
                    is_gpu_assigned = False
                    for i, schedule in enumerate(company['schedule']):
                        schedule_start = datetime.datetime.strptime(schedule['date'], "%Y-%m-%d").date()
                        schedule_end = datetime.datetime.strptime(company['schedule'][i+1]['date'], "%Y-%m-%d").date() if i+1 < len(company['schedule']) else datetime.date.max
                        
                        # 期間の重なりをチェック
                        if (schedule_start <= end_date and start_date < schedule_end) and schedule['assigned_gpu_node'] > 0:
                            is_gpu_assigned = True
                            break
                    if is_gpu_assigned:
                        should_remove_tag = True
                        break

            if should_remove_tag:
                # latest_tagを削除
                tags.remove(latest_tag)
                
                # タグを更新
                run.tags = tags
                run.update()
                print(f"Removed '{latest_tag}' tag from run: {run.id}")
                if matching_companies:
                    print(f"Matching companies: {', '.join([company['company'] for company in matching_companies])}")
                if 'overall' in tags:
                    print("'overall' tag present")
                removed_count += 1

    print(f"Process completed. Removed '{latest_tag}' tag from {removed_count} runs.")

if __name__ == "__main__":
    date_range = ["2024-10-25", "2024-10-25"]
    remove_latest_tags(date_range)