# GPU Dashboard for Geniac4

GPU使用率監視・レポート生成システム

## 概要

このプロジェクトは、WandB（Weights & Biases）のAPIを使用して、各チームのGPU使用状況を監視し、レポートを生成するシステムです。

## 主な機能

- **GPU使用率の自動計算**: WandBのrunデータからGPU使用率を計算
- **チーム別レポート生成**: 各チーム・企業ごとの詳細なGPU使用状況レポート
- **週次・月次集計**: 定期的な使用状況の集計とトレンド分析
- **自動アラート機能**: ダッシュボードの異常検知とSlack通知
- **CI/CD パイプライン**: GitHub Actionsによる自動デプロイ

## システム構成

```
gpu_dashboard/
├── main.py                    # メインエントリーポイント
├── config.yaml               # システム設定ファイル
├── requirements.txt          # Python依存関係
├── Dockerfile.main          # メインアプリケーション用Docker
├── Dockerfile.check_dashboard # ヘルスチェック用Docker
├── src/
│   ├── tracker/             # WandB データ取得・管理
│   ├── calculator/          # GPU使用率計算ロジック
│   ├── uploader/           # データアップロード処理
│   ├── alart/              # アラート・通知機能
│   └── utils/              # ユーティリティ関数
└── .github/workflows/      # CI/CD ワークフロー
```

## セットアップ

### 1. 依存関係のインストール

```bash
pip install -r gpu_dashboard/requirements.txt
```

### 2. 設定ファイルの準備

`gpu_dashboard/config.yaml` を編集して、監視対象のチーム・プロジェクトを設定してください。

### 3. WandB認証

```bash
wandb login
```

### 4. ローカル実行

```bash
cd gpu_dashboard
python main.py --start-date 2025-09-01 --end-date 2025-09-18
```

## AWS ECSでのデプロイ

### 前提条件

- AWS CLI設定済み
- ECRリポジトリ作成済み
- ECS クラスター・サービス設定済み

### デプロイ手順

1. **Dockerイメージのビルド・プッシュ**
   ```bash
   # メインアプリケーション
   cd gpu_dashboard
   docker build -f Dockerfile.main -t gpu-dashboard-main .
   docker tag gpu-dashboard-main:latest <ECR_URI>/gpu-dashboard-main:latest
   docker push <ECR_URI>/gpu-dashboard-main:latest
   ```

2. **ECSタスク定義の更新**
   - メモリ: 16GB
   - CPU: 2 vCPU
   - 環境変数: WANDB_API_KEY

3. **EventBridgeスケジュール**
   - 毎日 00:00 JST に実行
   - cron式: `cron(0 15 * * ? *)`（UTC）

## GitHub Actions CI/CD

### ワークフロー

- **docker-build-push-main.yml**: メインアプリケーションの自動ビルド・デプロイ
- **docker-build-push-checkDashboard.yml**: ヘルスチェック機能の自動デプロイ
- **docker-build-push-runExistenceCheck.yml**: Run存在チェック機能の自動デプロイ

### 必要なSecrets

GitHub リポジトリの Settings > Secrets に以下を設定：

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`: `us-east-2`

## 設定

### チーム設定例

```yaml
companies:
  - company: example-team
    teams:
      - example-team
    schedule:
      - date: "2025-08-01"
        assigned_gpu_node: 8
    include_project_pattern: "gpu-info"
```

### GPU計算ロジック

- **自動検出**: `runInfo.gpuCount` を使用
- **手動設定**: `world_size` パラメータを使用
- **固定割り当て**: `assigned_gpu_node` で指定

## トラブルシューティング

### よくある問題

1. **WandB API タイムアウト**
   - `run_manager.py` の `timeout` 値を調整
   - `samples` パラメータを削減

2. **ECS メモリ不足**
   - タスク定義のメモリ設定を増加
   - 16GB推奨

3. **重複タグ問題**
   - ECS サービスの重複実行を確認
   - 不要なサービスを削除

## ライセンス

[ライセンス情報を追加]

## コントリビューション

[コントリビューションガイドラインを追加]
