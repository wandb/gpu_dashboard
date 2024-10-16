import wandb
from src.utils.config import CONFIG

def remove_latest_tags():
    # W&B APIの初期化
    api = wandb.Api()

    # CONFIGからentity、project、latest_tagを取得
    entity = CONFIG.dashboard.entity
    project = CONFIG.dashboard.project
    latest_tag = CONFIG.dashboard.tag_for_latest

    # CONFIGから会社名のリストを作成
    company_names = [company['company'] for company in CONFIG.companies]

    # 'latest'タグを持つrunを取得
    runs = api.runs(f"{entity}/{project}", {"tags": {"$in": [latest_tag]}})

    removed_count = 0
    for run in runs:
        # runのタグを取得
        tags = run.tags
        
        # latest_tagと（会社名のいずれかまたは'overall'）がタグに含まれているか確認
        matching_companies = [company for company in company_names if company in tags]
        if latest_tag in tags and (matching_companies or 'overall' in tags):
            # latest_tagを削除
            tags.remove(latest_tag)
            
            # タグを更新
            run.tags = tags
            run.update()
            print(f"Removed '{latest_tag}' tag from run: {run.id}")
            if matching_companies:
                print(f"Matching companies: {', '.join(matching_companies)}")
            if 'overall' in tags:
                print("'overall' tag present")
            removed_count += 1

    print(f"Process completed. Removed '{latest_tag}' tag from {removed_count} runs.")

if __name__ == "__main__":
    remove_latest_tags()