# gpu-dashboard
## アーキテクチャ
![アーキテクチャ](./image/gpu-dashboard.drawio.png)
## このリポジトリのディレクトリ構成
```
.
├── README.md
├── README2.md
├── check-dashboard
│   ├── Dockerfile
│   ├── check_dashboard.py
│   ├── config.yaml
│   └── requirements.txt
├── gpu-dashboard
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
## デプロイ
### イメージのプッシュ
### ECR
### VPC
### ECS
## デバッグ

https://qiita.com/RyoMar/items/06e23d60d9df2d955221
https://qiita.com/ramunauna/items/f52cdcaeadedf40e5d22
https://note.com/yuta_shimada/n/n1563c94594ab