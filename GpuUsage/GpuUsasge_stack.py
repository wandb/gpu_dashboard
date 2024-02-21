from aws_cdk import (
    aws_events as events,
    aws_lambda as lambda_,
    aws_iam as aws_iam,
    aws_events_targets as targets,
    Duration,
    Stack,
)
from constructs import Construct
import os

from dotenv import load_dotenv

del os.environ["WANDB_API_KEY"]
load_dotenv()


class GpuUsageStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        # 1. IAM poloicy (for Lambda Function)
        lambdaRole = aws_iam.Role(
            self,
            "lambdaRole",
            role_name="GpuUsage-lambda-role",
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
        )
        lambdaRole.add_managed_policy(
            aws_iam.ManagedPolicy.from_aws_managed_policy_name("AWSLambdaExecute")
        )
        # lambdaRole.add_managed_policy(
        #     aws_iam.ManagedPolicy.from_aws_managed_policy_name("追加したいManagedのRoleがあれば")
        # )
        # 2. lambda configuration
        lambdaFn = lambda_.DockerImageFunction(
            self,
            "GpuUsageFunction",  # リソース名
            timeout=Duration.minutes(15),  # タイムアウトを10分に設定
            code=lambda_.DockerImageCode.from_image_asset("lambda/"),
            role=lambdaRole,
            retry_attempts=0,
            environment={
                "WANDB_API_KEY": os.getenv("WANDB_API_KEY"),
                "WANDB_DATA_DIR": "/tmp",
                "WANDB_DIR": "/tmp",
                "WANDB_CACHE_DIR": "/tmp",
            },
            memory_size=10240,
        )
        # 3. cron configuration
        rule = events.Rule(
            self,
            "Rule",
            schedule=events.Schedule.cron(
                minute="0",  # 100%にならないかもしれないので0分に設定
                hour="15",  # 時差があるのでマイナス9時間にする（24時の場合は15時に設定）
                day="*",
                month="*",
                year="*",
            ),
        )
        rule.add_target(targets.LambdaFunction(lambdaFn))
