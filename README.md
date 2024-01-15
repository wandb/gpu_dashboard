# gpu-dashboard
<!-- プロジェクト名を記載 -->

このリポジトリは、AWS CDKを使用してインフラストラクチャリソースをプロビジョニングし、GPU dashboardを定期的に更新するLambda関数を作成します。

## 環境
<!-- 言語、フレームワーク、ミドルウェア、インフラの一覧とバージョンを記載 -->

| Language, Framework   | Version       |
| --------------------- | ------------- |
| Python                | 3.11.6        |
| Docker                | 3.14.0        |
| Node.js               | 18.18.0       |
| aws-cli               | 1.29.61       |

| Specifications        | Version       |
| --------------------- | ------------- |
| macOS                 | Sonoma 14.1.2 |
| Chipset               | Apple M1      |

上記の環境で動作確認をしております。バージョンを厳密に一致させる必要ありませんが、デバッグ時の参考情報としてお使い下さい。その他のパッケージのバージョンは`requirements.txt`を参照してください。

## ディレクトリ構成
<!-- Treeコマンドを使ってディレクトリ構成を記載 -->

```
.
├── GpuUsage　# リソースのプロビジョニング
│   ├── GpuUsasge_stack.py
│   └── __init__.py
├── lambda  # Dockerのコンテナイメージ作成
│   ├── Dockerfile
│   ├── GpuUsage.py  # Lambdaに実行させる処理を記載
│   ├── config.yaml
│   └── requirements.txt
├── tests  # テストコード（未整備）
│   ├── __init__.py
│   └── unit
│       ├── __init__.py
│       └── test_GpuUsage_stack.py
├── .env  # 各自で作成
├── README.md
├── app.py
├── cdk.json  # プロビジョニングの設定ファイル
├── requirements-dev.txt
├── requirements.txt
└── source.bat  # Windows用のバッチファイル
```

## 開発環境構築
<!-- コンテナの作成方法、パッケージのインストール方法など、開発環境構築に必要な情報を記載 -->

### 環境準備
1. .env ファイルを作成し以下の内容を記載する

```
WANDB_API_KEY="YOUR_API_KEY"
```

2. Dockerを起動する

### プロビジョニング・デプロイ
```bash
cdk bootstrap  --qualifier rf4849cat
cdk deploy  --qualifier rf4849cat
```

### リソースの削除
```bash
cdk destroy
```
> bootstrapやdeployに失敗する場合は、既存のCloudFormationスタックの修飾子が競合している可能性があります。cdk.json最終行に記載の修飾子を```rf4849cat```から```rf4849dog```に修正するなどして再度試してみください。修正後はコマンドの変更を忘れないこと

### 動作確認

AWS Lambdaにアクセスしてテストをクリックすることで動作確認ができます。実行には数分を要します。

## 運用監視

1. AWS CloudWatchにアクセスする
2. ロググループをクリックする
3.  "GpuUsageStack"で検索しクリックする
4. ログストリームから見たいログをクリックする
5. タイムスタンプ、メッセージを確認する
