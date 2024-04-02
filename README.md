# gpu-dashboard

This repository provisions infrastructure resources using AWS CDK and creates a Lambda function to periodically update the GPU dashboard.

## Environment

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

The repository has been tested in the above environment. It is not necessary to match the versions exactly, but please use this information as a reference when debugging. For versions of other packages, please refer to `requirements.txt`.

## Directory Structure

```
.
├── README.md
└── gpu-dashboard
    ├── Dockerfile
    ├── blacklist
    ├── requirements.txt
    ├── config.yaml
    ├── GpuUsage.py
    ├── blank_table.py
    ├── config.py
    ├── fetch_runs.py
    ├── handle_artifacts.py
    ├── remove_tags.py
    ├── update_blacklist.py
    └── update_tables.py
```

## Development Environment Setup

### Environment Preparation
1. Ensure that the aws cli is configured with the appropriate credentials and region

```bash
$ aws configure list

      Name                    Value             Type    Location
      ----                    -----             ----    --------
   profile                <not set>             None    None
access_key     ******************** shared-credentials-file    
secret_key     ******************** shared-credentials-file    
    region           ap-northeast-1      config-file    ~/.aws/config
```

2. For debugging, create a Python virtual environment
```bash
$ cd gpu-dashboard
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip install -r requirements.txt
```

3. Go to AWS console and move to Elastic Container Registry

- ECR作成
Amazon ECR > プライベートレジストリ > リポジトリ
geniac-gpuという名前のリポジトリを作ります
作成したリポジトリに移動し、"プッシュコマンドの表示"をクリックします
4つのコマンドが表示されるので順番に実行します。

コマンド例
```bash
$ aws ecr get-login-password --region ap-northeast-1 | docker login --username AWS --password-stdin 111122223333.dkr.ecr.ap-northeast-1.amazonaws.com
$ docker build -t geniac-gpu .
$ docker tag geniac-gpu:latest 111122223333.dkr.ecr.ap-northeast-1.amazonaws.com/geniac-gpu:latest
$ docker push 111122223333.dkr.ecr.ap-northeast-1.amazonaws.com/geniac-gpu:latest
```

https://dev.classmethod.jp/articles/eventbridge-scheduler-regularly-start-and-stop-ecs-tasks/

- VPC作成
VPCに移動します
VPC作成
サブネット作成

- ECS
クラスター作成
タスク定義
タスク実行ロール


### Provisioning & Deployment
Start Docker and perform provisioning and deployment
```bash
$ cdk bootstrap  --qualifier rf4849cat
$ cdk deploy  --qualifier rf4849cat
```
> If there are code changes, execute `cdk deploy  --qualifier rf4849cat` to reflect the changes.

### Resource Deletion
```bash
$ cdk destroy
```
> If bootstrap or deploy fails, it may be due to a conflict with an existing CloudFormation stack qualifier. Try changing the qualifier at the end of cdk.json from `rf4849cat` to `rf4849dog` and try again. Don't forget to change the command after the modification.

### Operation Verification

You can verify the operation by clicking Test on AWS Lambda. It may take a few minutes to execute.

## Operational Monitoring

1. Access AWS CloudWatch
2. Click on Log Groups
3. Search for "GpuUsageStack" and click on the corresponding log group
4. Click on the log stream you want to view
5. Check the timestamp and message

## Local Debugging

### If executing the Python script directly

```bash
$ cd lambda
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip install -r requirements.txt
$ python3 GpuUsage.py
```
### If executing via Docker

1. Host Lambda locally

```bash
$ cd lambda
$ docker build --platform linux/amd64 -t docker-image:test .
$ docker run --platform linux/amd64 -p 9000:8080 docker-image:test
```

2. Call the API from another Terminal 
```
$ curl "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
```
> TODO Consider a solution for the error caused by the CPU Check of Polars on M1 Mac