# gpu-dashboard 
Extract gpu usage across the teams.

## Architecture
![Architecture](./image/gpu-dashboard.drawio.png)

## Directory Structure of this Repository
```
.
├── README.md 
├── check-dashboard  # for monitoring cron
│   ├── Dockerfile
│   ├── check_dashboard.py
│   ├── config.yaml 
│   └── requirements.txt
├── gpu-dashboard  # for cron
│   ├── Dockerfile
│   ├── GpuUsage.py
│   ├── blacklist
│   ├── blank_table.py
│   ├── config.py
│   ├── config.yaml
│   ├── fetch_runs.py
│   ├── handle_artifacts.py 
│   ├── remove_tags.py
│   ├── requirements.txt
│   ├── run.py
│   ├── update_blacklist.py  
│   └── update_tables.py
└── image
    └── gpu-dashboard.drawio.png
```

## Local Environment Setup
Execute below command in both check-dashboard directory and gpu-dashboard directory.
```
$ python3 -m venv .venv
$ . .venv/bin/activate  
$ pip install -r requirements.txt
```

## AWS Environment Setup
### Account Provisioning & Permissions
Get an AWS account from the administrator and grant access permissions to the following services in `IAM`:
- AWSBatch
- CloudWatch
- EC2  
- ECS
- ECR
- EventBridge
- IAM
- VPC

### AWS CLI Configuration 
Create a user for AWS CLI in `IAM`. Grant access permissions to the following services:
- ECR

Click on the created user, note down the following strings in the Access key tab:
- Access key ID
- Secret access key

Execute the following command in the local Terminal to log in to AWS.

```shell
$ aws configure

AWS Access Key ID [None]: Access key ID
# Enter  
AWS Secret Access Key [None]: Secret access key
# Enter
Default region name [None]: blank
# Enter
Default output format [None]: blank  
# Enter
```

After the configuration is completed, execute the following command to check the connection. If successful, the file list of s3 will be output.
```shell 
$ aws s3 ls
```
Reference: [【AWS】How to set up aws cli](https://zenn.dev/akkie1030/articles/aws-cli-setup-tutorial)

## Deploying Periodic Execution Programs
### ECR
#### Creating Repositories
- Go to `Amazon ECR > Private registry > Repositories` 
- Click on `Create repository`
- Enter any repository name in `Repository name` (e.g. geniac-gpu)
- Click `Create repository`

#### Pushing Images
- Click on the created repository name
- Click on `View push commands` 
- Execute the 4 commands displayed in the local Terminal in order

```bash
# Example commands  
$ aws ecr get-login-password --region ap-northeast-1 | docker login --username AWS --password-stdin 111122223333.dkr.ecr.ap-northeast-1.amazonaws.com
$ docker build -t geniac-gpu .
$ docker tag geniac-gpu:latest 111122223333.dkr.ecr.ap-northeast-1.amazonaws.com/geniac-gpu:latest
$ docker push 111122223333.dkr.ecr.ap-northeast-1.amazonaws.com/geniac-gpu:latest  
```

> Since commands are uniquely determined by the repository, it's easy to deploy from the second time onward by writing the above commands in a shell script.

Perform the above procedure in both the `gpu-dashboard` directory and the `check-dashboard` directory.

### VPC
- Go to `Virtual Private Cloud > Your VPCs`
- Click on `Create VPC`
- Select `VPC and more` from `Resources to create`
- Click on `Create VPC`

### IAM  
- Go to `IAM > Roles`
- Click on `Create role`
- Configure `Use case` settings
    - Select `Elastic Container Service` for `Service`
    - Select `Elastic Container Service Task` for `Use case`
- Select `AmazonEC2ContainerRegistryReadOnly` and `CloudWatchLogsFullAccess` in `Permissions policies`
- Click `Next`  
- Enter `ecsTaskExecutionRole` in `Role name`
- Click `Create role`

### ECS
#### Creating Clusters
- Go to `Amazon Elastic Container Service > Clusters`
- Click on `Create cluster`
- Enter any cluster name in `Cluster name`
- Click on `Create`

#### Task Definition
- Go to `Amazon Elastic Container Service > Task Definitions`
- Click on `Create new Task Definition`, then click on `Create new Task Definition` 
- Enter any task definition family name in `Task definition family`
- Change `CPU` and `Memory` in `Task size` as needed
- Select `ecsTaskExecutionRole` in `Task role`
- Configure `Container - 1` settings
    - Enter the pushed repository name and image URI in ECR in `Container details`
    - Set `Resource allocation limits` appropriately according to the `Task size`
- Click `Add environment variable` in `Environment variables - Optional` and add the following:
    - Key: WANDB_API_KEY
    - Value: {Your WANDB_API_KEY}
- Click `Create`

Perform the above procedure in both the `gpu-dashboard` directory and the `check-dashboard` directory.

#### Creating Tasks  
- Go to `Amazon Elastic Container Service > Clusters > {Cluster name} > Scheduled tasks`
- Click on `Create`
- Enter any rule name in `Scheduled rule name`
- Select `cron expression` from `Scheduled rule type`
- Enter an appropriate expression in `cron expression`
    - Since this UI requires input in UTC time, `cron(15 15 * * ? *)` corresponds to 0:15 a.m. Japan time.
- Enter any target ID in `Target ID`
- Select the task definition from `Task definition family`  
- Select VPC and subnet in `Networking`
- If there is no existing security group in `Security groups`, select `Create new security group` and create a security group
- Click on `Create`

Perform the above procedure in both the `gpu-dashboard` directory and the `check-dashboard` directory.

## Debugging
### Local Environment Setup
Execute the following command to set up the python environment for the periodic execution script.
It's good to create a file called `debug.ipynb` for debugging.  
By editing `config.yaml`, you can reduce the impact on the production environment.

```shell
$ cd gpu-dashboard
$ python3 -m venv .venv
$ . .venv/bin/activate
```

Similarly, execute the following command to set up the python environment for the periodic execution check script.
It's also good to create a file called `debug.ipynb` in this directory for debugging.

```shell  
$ cd check-dashboard
$ python3 -m venv .venv 
$ . .venv/bin/activate
```

### How to Check Logs
- Go to `CloudWatch > Log groups` in AWS
- Click on `/ecs/{Task definition name}`  
- Click on the log stream to check the logs

## Appendix
### Update Blacklist
If you forget to add the `ignore_tag` during regular execution, runs will continue to be counted towards GPU usage. To exclude them from the calculation, add the tag and run the script.

```shell
$ cd gpu-dashboard
$ python3 -m venv .venv
$ . .venv/bin/activate
$ python3 update_blacklist.py
```

### Program Processing Procedure
- Fetch latest data
    - Set target_date
    - Create a list of companies  
    - Get projects for each company [Public API]
    - Get runs for each project [Private API]
        - Filter by target_date and tags
    - Detect and alert runs that call wanb.init multiple times on the same instance
    - Get system metrics for each run [Public API]  
    - Aggregate by run id x date
- Update data  
    - Get csv up to yesterday from Artifacts
    - Concat latest data and save to Artifacts
    - Filter run ids
- Aggregation
    - Aggregate overall  
    - Aggregate monthly
    - Aggregate daily company
- Update tables
    - Reset latest tag
    - Output tables