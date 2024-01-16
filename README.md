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

2. aws cliに適切な資格情報とリージョンが設定されていることを確認する

```bash
$ aws configure list

      Name                    Value             Type    Location
      ----                    -----             ----    --------
   profile                <not set>             None    None
access_key     ******************** shared-credentials-file    
secret_key     ******************** shared-credentials-file    
    region           ap-northeast-1      config-file    ~/.aws/config
```


3. CDKを初めて使用する場合はセットアップをする
```
$ npm install -g aws-cdk
```

4. Pythonの仮想環境を作成する
```bash
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip install -r requirements.txt
```



### プロビジョニング・デプロイ
Dockerを起動しプロビジョニングとデプロイを行う
```bash
$ cdk bootstrap  --qualifier rf4849cat
$ cdk deploy  --qualifier rf4849cat
```
> コードの変更の場合は`cdk deploy  --qualifier rf4849cat`を実行すると変更が反映されます。

### リソースの削除
```bash
$ cdk destroy
```
> bootstrapやdeployに失敗する場合は、既存のCloudFormationスタックの修飾子が競合している可能性があります。cdk.json最終行に記載の修飾子を```rf4849cat```から```rf4849dog```に修正するなどして再度試してみてください。修正後はコマンドの変更を忘れないこと

### 動作確認

AWS Lambdaにアクセスしてテストをクリックすることで動作確認ができる。実行には数分を要する。

## 運用監視

1. AWS CloudWatchにアクセスする
2. ロググループをクリックする
3.  "GpuUsageStack"で検索し該当のロググループをクリックする
4. ログストリームから見たいログをクリックする
5. タイムスタンプ、メッセージを確認する

## ローカルデバッグ

### Pythonスクリプトを直接実行する場合

```bash
$ cd lambda
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip install -r requirements.txt
$ python3 GpuUsage.py
```
### Docker経由で実行する場合

1. Lambdaをローカルでホストする

```bash
$ cd lambda
$ docker build --platform linux/amd64 -t docker-image:test .
$ docker run --platform linux/amd64 -p 9000:8080 docker-image:test
```

2. 別のTerminalからAPIをコールする 
```
$ curl "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
```
> TODO M1 MacではPolarsのCPU Checkでエラーが出てしまうので解決策を考える