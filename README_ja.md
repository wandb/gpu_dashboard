# gpu-dashboard
このリポジトリは、GPU使用状況を追跡し、ダッシュボードを生成するためのツールです。

## 主な機能
1. 複数の企業やプロジェクトからGPU使用データを収集
2. 日次、週次、月次、および全期間のGPU使用状況レポートを生成
3. Weights & Biases (wandb) を使用してダッシュボードを更新
4. 異常なGPU使用率の検出とアラート機能

## Architecture
![Architecture](./image/gpu-dashboard.drawio.png)

## このリポジトリのディレクトリ構成
```
.
├── Dockerfile.check_dashboard
├── Dockerfile.main
├── README.md
├── config.yaml
├── main.py
├── requirements.txt
├── src
│   ├── alart
│   │   └── check_dashboard.py
│   ├── calculator
│   │   ├── blank_table.py
│   │   ├── gpu_usage_calculator.py
│   │   └── remove_tags.py
│   ├── tracker
│   │   ├── common.py
│   │   ├── config_parser.py
│   │   ├── run_manager.py
│   │   └── set_gpucount.py
│   ├── uploader
│   │   ├── artifact_handler.py
│   │   ├── data_processor.py
│   │   └── run_uploader.py
│   └── utils
│       └── config.py
└── image
    └── gpu-dashboard.drawio.png
```

## ローカルの環境構築
gpu-dashboard ディレクトリで以下のコマンドを実行します。
```
$ python3 -m venv .venv
$ . .venv/bin/activate
$ pip install -r requirements.txt
```

## AWS環境構築
### アカウントの払い出し・権限付与
管理者からAWSアカウントを払い出してもらい、`IAM`にて下記サービスにアクセス権限を付与する
- AWSBatch
- CloudWatch
- EC2
- ECS
- ECR
- EventBridge
- IAM
- VPC

### AWS CLI設定
`IAM`にてAWS CLI用のユーザーを作成する。下記サービスにアクセス権限を付与する。
- ECR

作成したユーザーをクリックし、アクセスキーのタブにて以下の文字列を控える
- Access key ID
- Secret access key
  
下記コマンドをローカルのTerminalで実行し、AWSにログインする。

```shell
$ aws configure

AWS Access Key ID [None]: Access key ID
# Enter
AWS Secret Access Key [None]: Secret access key
# Enter
Default region name [None]: 未入力
# Enter
Default output format [None]: 未入力
# Enter
```

設定が完了したら下記コマンドで疎通を確認する。成功するとs3のファイル一覧が出力される。
```shell
$ aws s3 ls
```
参考: [【AWS】aws cliの設定方法](https://zenn.dev/akkie1030/articles/aws-cli-setup-tutorial)

## 定期実行プログラムのデプロイ
### ECR
#### リポジトリの作成
- `Amazon ECR > プライベートレジストリ > リポジトリ`に移動する
- `リポジトリ作成`をクリックする
- `リポジトリ名`に任意のリポジトリ名を入力する（例: geniac-gpu）
- `リポジトリを作成`をクリックする
#### イメージのプッシュ
- 作成したリポジトリ名をクリックする
- `プッシュコマンドの表示`をクリックする
- 表示される4つのコマンドをローカルのTerminalで順に実行する  


```bash
# コマンド例
$ aws ecr get-login-password --region ap-northeast-1 | docker login --username AWS --password-stdin 111122223333.dkr.ecr.ap-northeast-1.amazonaws.com
$ docker build -t geniac-gpu .
$ docker tag geniac-gpu:latest 111122223333.dkr.ecr.ap-northeast-1.amazonaws.com/geniac-gpu:latest
$ docker push 111122223333.dkr.ecr.ap-northeast-1.amazonaws.com/geniac-gpu:latest
```
> コマンドはリポジトリで一意に決まるため上記コマンドをShellスクリプトに記載することで2回目以降は簡単にデプロイできます

上記手順で`gpu-dashboard`と`check-dashboard`それぞれのリポジトリを作成する

### VPC
- `仮想プライベートクラウド > お使いのVPC`に移動する
- `VPCを作成`をクリックする
- `作成するリソース`から`VPCなど`を選択する
- `VPCを作成`をクリックする

### IAM
- `IAM > ロール`に移動する
- `ロールを作成`をクリックする
- `ユースケース`の設定をする
    - `サービス`で`Elastic Container Service`を選択する
    - `ユースケース`で`Elastic Container Service Task`を選択する
- `許可ポリシー`で`AmazonEC2ContainerRegistryReadOnly`と`CloudWatchLogsFullAccess`を選択する
- `次へ`をクリックする
- `ロール名`に`ecsTaskExecutionRole`と入力する
- `ロールを作成`をクリックする

### ECS
#### クラスタ作成
- `Amazon Elastic Container Service > クラスター`に移動する
- `クラスターの作成`をクリックする
- `クラスター名`に任意のクラスタ名を入力する
- `作成`をクリックする

#### タスク定義
- `Amazon Elastic Container Service > タスク定義`に移動する
- `新しいタスク定義の作成`をクリックし、`新しいタスク定義の作成`をクリックする
- `タスク定義ファミリー`に任意のタスク定義ファミリー名を入力する
- `タスクサイズ`の`CPU`と`メモリ`を必要に応じて変更する
- `タスクロール`で`ecsTaskExecutionRole`を選択する
- `コンテナ - 1`の設定をする
    - `コンテナの詳細`にECRにプッシュしたリポジトリ名とイメージURIを入力する
    - `リソース割り当て制限`を`タスクサイズ`に応じて適切に設定する
- `環境変数 - オプション` の`環境変数を追加`をクリックし、以下を追加する
    - キー: WANDB_API_KEY
    - 値: {Your WANDB_API_KEY}
- `作成`をクリックする

#### タスク作成
- `Amazon Elastic Container Service > クラスター > {クラスター名} > スケジュールされたタスク`に移動する
- `作成`をクリックする
- `スケジュールされたルールの名前`に任意のルール名を入力する
- `スケジュールされたルールのタイプ`の`cron 式`を選択する
- `cron 式`に適切な式を入力する
    - このUIではUTC時間で入力する必要があるため、`cron(15 15 * * ? *)`は日本時間の0時15分となる
- `ターゲットID`に任意のターゲットIDを入力する
- `タスク定義ファミリー`からタスク定義を選択する
- `ネットワーキング`でVPCとサブネットを選択する
- `セキュリティグループ`に既存のセキュリティグループがなければ`新しいセキュリティグループの作成`を選択しセキュリティグループを作成する
- `作成`をクリックする

## デバッグ
### ローカルの環境構築
下記のコマンドを実行して、定期実行スクリプトのローカル実行python環境を構築する。
`config.yaml`を編集することで本番環境への影響を抑えられる。

```shell
$ cd gpu-dashboard
$ python3 -m venv .venv
$ . .venv/bin/activate
```

### 使用方法
#### メインスクリプトの実行
```shell
python main.py [--api WANDB_API_KEY] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]
```
--api: wandb APIキー（オプション、環境変数で設定可能）
--start-date: データ取得開始日（オプション）
--end-date: データ取得終了日（オプション）

#### ダッシュボードの健全性チェック
```shell
python src/alart/check_dashboard.py
```

### 主要コンポーネント
- src/tracker/: GPU使用データの収集
- src/calculator/: GPU使用統計の計算
- src/uploader/: wandbへのデータアップロード
- src/alart/: 異常検出とアラート機能

### ログの確認方法
- AWSで`CloudWatch > ロググループ`に移動する
- `/ecs/{タスク定義名}`をクリックする
- ログストリームをクリックしてログを確認する

## Appendix
### プログラムの処理手順
- 最新データ取得(src/tracker/)
    - start_dateとend_dateを設定
        - 未指定の場合はデフォルトで両方の値が昨日の日付に設定される
    - companyのリストを作成
    - companyごとにprojectを取得[Public API]
    - projectごとにrunを取得[Private API]
        - target_date、tagsフィルタリング
    - 同じインスタンスで複数回wanb.initをしているrunを検出しアラート
    - runごとにsystem metricsを取得[Public API]
    - run id x 日付で集計
- データ更新(src/uploader/)
    - 昨日までのcsvをArtifactsから取得
    - 最新分をconcatしてArtifactsに保存
    - run idのフィルタリング
- データの集計と更新(src/calculator)
    - latestタグの削除
    - 取得したデータについて集計
        - 全体のデータを集計
        - 月次のデータを集計
        - 週次のデータを集計
        - 日次のデータを集計
        - サマリーデータを集計
    - overallテーブルを更新
    - 企業毎のテーブルを更新

### 分散処理のGPU数の計算例
src/tracker/run_manager.py内の__set_gpucountメソッドにおいて、
異なるチームや設定に基づいて分散処理時のGPU数を計算します。以下に、計算方法と具体例を示します。

## 1. num_nodesとnum_gpusの値がconfigに含まれている場合

### 計算方法
- `num_nodes` と `num_gpus` の値を取得し、それらを掛け合わせてGPU数を計算します。
- これらの値は設定ファイル内の `config` セクションから取得されます。

### 具体例
config = {
    "num_nodes": 2,
    "num_gpus": 8
}

gpu_count = 2 * 8 = 16

この例では、2つのノードがあり、各ノードに8つのGPUがあるため、合計GPU数は16となります。

## 2. world_sizeがconfigに含まれている場合

### 計算方法
- `world_size` の値を使用してGPU数を決定します。
- `world_size` は設定ファイル内の `config` セクションから取得されます。

### 具体例
config = {
    "world_size": 16
}

gpu_count = 16

この例では、`world_size` の値がそのままGPU数として使用されます。

## 3. configから分散処理の設定が取得できない場合

### 計算方法
- `node.runInfo.gpuCount` の値を使用します。

node.runInfo = {
    "gpuCount": 8
}

gpu_count = 8

この例では、`runInfo` から直接GPU数を取得しています。

## 注意事項
このメソッドは、様々な形式の設定に対応し、可能な限り正確なGPU数を計算することを目指しています。ただし、予期しない形式のデータに遭遇した場合は、安全のためにGPU数を0とし、警告を出力します。