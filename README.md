# gpu-dashboard
Extract gpu usage across the teams.
## Architecture
![Architecture](./image/gpu-dashboard.drawio.png)

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
Execute below command in both check-dashboard directory and gpu-dashboard directory.
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

上記手順を`gpu-dashboard`ディレクトリと`check-dashboard`ディレクトリにてそれぞれ行うこと

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

上記手順を`gpu-dashboard`ディレクトリと`check-dashboard`ディレクトリにてそれぞれ行うこと

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

上記手順を`gpu-dashboard`ディレクトリと`check-dashboard`ディレクトリにてそれぞれ行うこと

## Appendix
### プログラムの処理手順
- 最新データ取得
    - target_dateを設定
    - companyのリストを作成
    - companyごとにprojectを取得[Public API]
    - projectごとにrunを取得[Private API]
        - target_date、tagsフィルタリング
    - 同じインスタンスで複数回wanb.initをしているrunを検出しアラート
    - runごとにsystem metricsを取得[Public API]
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

### デバッグ
work in progress ...

### ログの確認方法
work in progress ...