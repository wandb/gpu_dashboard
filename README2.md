# gpu-dashboard
## アーキテクチャ
![アーキテクチャ](./image/gpu-dashboard.drawio.png)

## このリポジトリのディレクトリ構成
```
.
├── README.md
├── check-dashboard  # for monitoring cron
│   ├── Dockerfile
│   ├── check_dashboard.py
│   ├── config.yaml
│   └── requirements.txt
├── gpu-dashboard  # for cron
│   ├── Dockerfile
│   ├── GpuUsage.py
│   ├── blacklist
│   ├── blank_table.py
│   ├── config.py
│   ├── config.yaml
│   ├── fetch_runs.py
│   ├── handle_artifacts.py
│   ├── remove_tags.py
│   ├── requirements.txt
│   ├── run.py
│   ├── update_blacklist.py
│   └── update_tables.py
└── image
    └── gpu-dashboard.drawio.png
```

## ローカルの環境構築
Execute below command in check-dashboard directory and gpu-dashboard directory.
```
$ python3 -m venv .venv
$ . .venv/bin/activate
$ pip install requirements.txt
```

## プログラムの処理内容
- 最新データ取得
    - target_dateを設定
    - companyのリストを作成
    - companyごとにprojectを取得[API]
    - projectごとにrunを取得[API]
        - target_date、tagsフィルタリング
    - 同じインスタンスで複数回wanb.initをしているrunを検出しアラート
    - runごとにsystem metricsを取得[API]
    - run id x 日付で集計
- データ更新
    - 昨日までのcsvをArtifactsから取得
    - 最新分をconcatしてArtifactsに保存
    - run idのフィルタリング
- 集計
    - overallを集計
    - monthlyを集計
    - daily companyを集計
- テーブル更新
    - latestタグをリセット
    - テーブルを出力

### AWS
- アカウント払出し・権限付与
- IAM作成
- AWS CLIの疎通確認

#### ECR
- リポジトリ作成
- イメージのプッシュ
```bash
$ docker
```

#### VPC
- VPC作成
- サブネット作成

#### ECS
- クラスタ作成
- タスク定義
- タスク作成

## デバッグ
## ロギング
## チェックスクリプト

https://qiita.com/RyoMar/items/06e23d60d9df2d955221  
https://qiita.com/ramunauna/items/f52cdcaeadedf40e5d22  
https://note.com/yuta_shimada/n/n1563c94594ab  