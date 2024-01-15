# gpu-dashboard
<!-- プロジェクト名を記載 -->

このリポジトリは、AWS CDKを使用してインフラストラクチャリソースをプロビジョニングし、GPU dashboardを定期的に更新するLambda関数を作成します。

## 環境
<!-- 言語、フレームワーク、ミドルウェア、インフラの一覧とバージョンを記載 -->

| 言語・フレームワーク     | バージョン      |
| --------------------- | ------------- |
| チップ                 | Apple M1      |
| macOS                 | Sonoma 14.1.2 |
| Python                | 3.11.6        |
| Docker                | 3.14.0        |
| Node.js               | 18.18.0       |
| React                 | 18.2.0        |
| aws-cli               | 1.29.61       |

その他のパッケージのバージョンは`requirements.txt`を参照してください。

## ディレクトリ構成

<!-- Treeコマンドを使ってディレクトリ構成を記載 -->
.
├── README.md
├── app.py
├── requirements.txt
├── requirements-dev.txt
├── cdk.json
├── source.bat
├── .env
├── GpuUsage
│   ├── GpuUsasge_stack.py
│   └── __init__.py
├── lambda
│   ├── Dockerfile
│   ├── GpuUsage.py
│   ├── config.yaml
│   └── requirements.txt
└── tests
    ├── __init__.py
    └── unit
        ├── __init__.py
        └── test_GpuUsage_stack.py

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

AWS Lambdaにアクセスしてテストをクリックします。実行には数分を要します。

### コンテナの停止

以下のコマンドでコンテナを停止することができます

make down

### 環境変数の一覧

| 変数名                 | 役割                                      | デフォルト値                       | DEV 環境での値                           |
| ---------------------- | ----------------------------------------- | ---------------------------------- | ---------------------------------------- |
| MYSQL_ROOT_PASSWORD    | MySQL のルートパスワード（Docker で使用） | root                               |                                          |
| MYSQL_DATABASE         | MySQL のデータベース名（Docker で使用）   | django-db                          |                                          |
| MYSQL_USER             | MySQL のユーザ名（Docker で使用）         | django                             |                                          |
| MYSQL_PASSWORD         | MySQL のパスワード（Docker で使用）       | django                             |                                          |
| MYSQL_HOST             | MySQL のホスト名（Docker で使用）         | db                                 |                                          |
| MYSQL_PORT             | MySQL のポート番号（Docker で使用）       | 3306                               |                                          |
| SECRET_KEY             | Django のシークレットキー                 | secretkey                          | 他者に推測されないランダムな値にすること |
| ALLOWED_HOSTS          | リクエストを許可するホスト名              | localhost 127.0.0.1 [::1] back web | フロントのホスト名                       |
| DEBUG                  | デバッグモードの切り替え                  | True                               | False                                    |
| TRUSTED_ORIGINS        | CORS で許可するオリジン                   | http://localhost                   |                                          |
| DJANGO_SETTINGS_MODULE | Django アプリケーションの設定モジュール   | project.settings.local             | project.settings.dev                     |

### コマンド一覧

| Make                | 実行する処理                                                            | 元のコマンド                                                                               |
| ------------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| make prepare        | node_modules のインストール、イメージのビルド、コンテナの起動を順に行う | docker-compose run --rm front npm install<br>docker-compose up -d --build                  |
| make up             | コンテナの起動                                                          | docker-compose up -d                                                                       |
| make build          | イメージのビルド                                                        | docker-compose build                                                                       |
| make down           | コンテナの停止                                                          | docker-compose down                                                                        |
| make loaddata       | テストデータの投入                                                      | docker-compose exec app poetry run python manage.py loaddata crm.json                      |
| make makemigrations | マイグレーションファイルの作成                                          | docker-compose exec app poetry run python manage.py makemigrations                         |
| make migrate        | マイグレーションを行う                                                  | docker-compose exec app poetry run python manage.py migrate                                |
| make show_urls      | エンドポイントをターミナル上で一覧表示                                  | docker-compose exec app poetry run python manage.py show_urls                              |
| make shell          | テストデータの投入                                                      | docker-compose exec app poetry run python manage.py debugsqlshell                          |
| make superuser      | スーパーユーザの作成                                                    | docker-compose exec app poetry run python manage.py createsuperuser                        |
| make test           | テストを実行                                                            | docker-compose exec app poetry run pytest                                                  |
| make test-cov       | カバレッジを表示させた上でテストを実行                                  | docker-compose exec app poetry run pytest --cov                                            |
| make format         | black と isort を使ってコードを整形                                     | docker-compose exec app poetry run black . <br> docker-compose exec app poetry run isort . |
| make update         | Poetry 内のパッケージの更新                                             | docker-compose exec app poetry update                                                      |
| make app            | アプリケーション のコンテナへ入る                                       | docker exec -it app bash                                                                   |
| make db             | データベースのコンテナへ入る                                            | docker exec -it db bash                                                                    |
| make pdoc           | pdoc ドキュメントの作成                                                 | docker-compose exec app env CI_MAKING_DOCS=1 poetry run pdoc -o docs application           |
| make init           | Terraform の初期化                                                      | docker-compose -f infra/docker-compose.yml run --rm terraform init                         |
| make fmt            | Terraform の設定ファイルをフォーマット                                  | docker-compose -f infra/docker-compose.yml run --rm terraform fmt                          |
| make validate       | Terraform の構成ファイルが正常であることを確認                          | docker-compose -f infra/docker-compose.yml run --rm terraform validate                     |
| make show           | 現在のリソースの状態を参照                                              | docker-compose -f infra/docker-compose.yml run --rm terraform show                         |
| make apply          | Terraform の内容を適用                                                  | docker-compose -f infra/docker-compose.yml run --rm terraform apply                        |
| make destroy        | Terraform で構成されたリソースを削除                                    | docker-compose -f infra/docker-compose.yml run --rm terraform destroy                      |

### リモートデバッグの方法

リモートデバッグ を使用する際は以下の url を参考に設定してください<br>
[Django のコンテナへリモートデバッグしよう！](https://qiita.com/shun198/items/9e4fcb4479385217c323)

## トラブルシューティング

### .env: no such file or directory

.env ファイルがないので環境変数の一覧を参考に作成しましょう

### docker daemon is not running

Docker Desktop が起動できていないので起動させましょう

### Ports are not available: address already in use

別のコンテナもしくはローカル上ですでに使っているポートがある可能性があります
<br>
下記記事を参考にしてください
<br>
[コンテナ起動時に Ports are not available: address already in use が出た時の対処法について](https://qiita.com/shun198/items/ab6eca4bbe4d065abb8f)

### Module not found

make build

を実行して Docker image を更新してください

<p align="right">(<a href="#top">トップへ</a>)</p>


## リポジトリ概要
このリポジトリは、AWS CDKを使用してインフラストラクチャリソースをプロビジョニングし、GPU dashboardを定期的に更新するLambda関数を作成します。AWS CDK（Cloud Development Kit）は、クラウドインフラをコードで定義し、AWS CloudFormationを通じてプロビジョニングするためのオープンソースのソフトウェア開発フレームワークです。

## 前提条件

始める前に、以下がインストールされていることを確認してください：
- Node.js（npm）
- AWS CLI
- AWS CDK Toolkit

検証環境  
| OS |  | 日 | 時 | 分 |
|---|---|---|---|---|
| 2024 | 01 | 15 | 14 | 38 |


## セットアップ

1. **AWS CLIの設定**

   適切な認証情報とデフォルトリージョンでAWS CLIが設定されていることを確認してください。

   ```sh
   aws configure
   ```

2. **依存関係のインストール**

   プロジェクトディレクトリに移動し、必要なnpmパッケージをインストールします：

   ```sh
   npm install
   ```

3. **AWS環境のブートストラップ**

   このリージョンでAWS CDKを初めて使用する場合は、AWS環境をブートストラップする必要があります。

   ```sh
   cdk bootstrap
   ```

## スタックのデプロイ

AWSにスタックをデプロイするには、以下のコマンドを実行します：

```sh
cdk deploy
```

このコマンドは、CDKコードからCloudFormationテンプレートを合成し、AWSアカウントにリソースをデプロイします。

## Lambda関数の操作

スタックをデプロイした後、Lambda関数はAWSアカウントで利用可能になります。AWS CLIを使用して関数を呼び出すか、他のAWSサービスと統合することができます。

## クリーンアップ

将来的な料金を避けるために、使用後はリソースを破棄してください：

```sh
cdk destroy
```

これにより、このスタックによって作成されたリソースがAWSアカウントから削除されます。

## サポート

問題に遭遇した場合や支援が必要な場合は、プロジェクトリポジトリで問題を提起するか、プロジェクトのメンテナーに連絡してください。

---

このAWS CDKプロジェクトを使用してAWSリソースとLambda関数を管理していただき、ありがとうございます。コーディングを楽しんでください！



The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

Enjoy!
