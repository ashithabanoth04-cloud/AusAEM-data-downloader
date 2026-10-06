# =============================================================================
# AWS ECS Fargate One-Click Deployment & Cloud Runner for AusAEM-WA
# =============================================================================
#
# Automates complete cloud execution in AWS:
# 1. Builds Linux/AMD64 Docker container (if not --skip-build).
# 2. Pushes image to Amazon Elastic Container Registry (ECR).
# 3. Registers ECS Fargate Task Definition.
# 4. Launches the task on AWS ECS Fargate.
# 5. Exits cleanly while the task continues running in AWS Cloud.
#
# =============================================================================

import os
import sys
import time
import base64
import subprocess
import argparse
import boto3
from botocore.exceptions import ClientError

# Automatically load environment variables from .env
try:
    from dotenv import load_dotenv

    load_dotenv()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(script_dir, ".env"))

except ImportError:
    pass


AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
ECR_REPO_NAME = os.getenv("ECR_REPO_NAME", "ausaem-wa-worker")
ECS_CLUSTER_NAME = os.getenv("ECS_CLUSTER_NAME", "ausaem-fargate-cluster")
ECS_TASK_FAMILY = os.getenv("ECS_TASK_FAMILY", "ausaem-downloader-task")
S3_BUCKET = os.getenv("S3_BUCKET", "ausaem-wa-data")

CONTAINER_NAME = "ausaem-worker-container"


def get_account_id(sts_client):
    return sts_client.get_caller_identity()["Account"]


def ensure_ecr_repo(ecr_client, repo_name):
    try:
        res = ecr_client.describe_repositories(
            repositoryNames=[repo_name]
        )
        return res["repositories"][0]["repositoryUri"]

    except ClientError as e:
        if e.response["Error"]["Code"] == "RepositoryNotFoundException":
            print(
                f"[*] Creating ECR Repository '{repo_name}' "
                f"in {AWS_REGION}...",
                flush=True
            )

            res = ecr_client.create_repository(
                repositoryName=repo_name
            )

            return res["repository"]["repositoryUri"]

        raise


def get_ecr_login_command(ecr_client, registry_uri):
    token = ecr_client.get_authorization_token()

    auth_data = token["authorizationData"][0]

    auth_token = base64.b64decode(
        auth_data["authorizationToken"]
    ).decode("utf-8")

    user, password = auth_token.split(":")
    endpoint = auth_data["proxyEndpoint"]

    return user, password, endpoint


def build_and_push_docker(ecr_uri, tag="latest"):

    print(
        "\n[1/4] Building Docker image for Linux/AMD64 "
        "(ECS Fargate compatible)...",
        flush=True
    )

    full_image_uri = f"{ecr_uri}:{tag}"

    script_dir = os.path.dirname(
        os.path.abspath(__file__)
    )

    build_cmd = [
        "docker",
        "build",
        "--platform",
        "linux/amd64",
        "-t",
        full_image_uri,
        script_dir
    ]

    subprocess.run(build_cmd, check=True)

    print(
        "\n[2/4] Logging in to AWS ECR and pushing image...",
        flush=True
    )

    ecr_client = boto3.client(
        "ecr",
        region_name=AWS_REGION
    )

    user, password, endpoint = get_ecr_login_command(
        ecr_client,
        ecr_uri
    )

    login_proc = subprocess.Popen(
        [
            "docker",
            "login",
            "--username",
            user,
            "--password-stdin",
            endpoint
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    stdout, stderr = login_proc.communicate(
        input=password
    )

    if login_proc.returncode != 0:
        raise RuntimeError(
            f"Docker login to ECR failed: {stderr}"
        )

    print(
        "[+] Docker login successful. "
        "Pushing layers to AWS ECR...",
        flush=True
    )

    push_cmd = [
        "docker",
        "push",
        full_image_uri
    ]

    max_push_attempts = 10

    for push_attempt in range(
        1,
        max_push_attempts + 1
    ):

        try:

            print(
                f"[*] Uploading to ECR "
                f"(Attempt {push_attempt}/{max_push_attempts})...",
                flush=True
            )

            subprocess.run(
                push_cmd,
                check=True
            )

            print(
                f"[+] Docker image successfully pushed: "
                f"{full_image_uri}",
                flush=True
            )

            return full_image_uri

        except subprocess.CalledProcessError as e:

            if push_attempt < max_push_attempts:

                print(
                    "[!] Docker push had a transient "
                    "network timeout. Retrying in 3s...",
                    flush=True
                )

                time.sleep(3)

            else:

                raise RuntimeError(
                    f"Docker push failed after "
                    f"{max_push_attempts} attempts: {e}"
                )


def get_iam_execution_role_arn(account_id):
    return (
        f"arn:aws:iam::{account_id}:role/"
        f"ecsTaskExecutionRole"
    )


def ensure_cloudwatch_log_group(
    logs_client,
    log_group_name
):

    try:

        logs_client.create_log_group(
            logGroupName=log_group_name
        )

    except Exception:
        pass


def register_task_definition(
    ecs_client,
    execution_role_arn,
    image_uri,
    log_group_name=None,
    memory="8192",
    cpu="2048"
):

    print(
        f"\n[3/4] Registering ECS Fargate Task Definition "
        f"(CPU: {cpu}, RAM: {memory}MB)...",
        flush=True
    )

    session = boto3.Session()
    creds = session.get_credentials()

    env_vars = [
        {
            "name": "S3_BUCKET",
            "value": S3_BUCKET
        },
        {
            "name": "RAW_PREFIX",
            "value": os.getenv(
                "RAW_PREFIX",
                "raw"
            )
        },
        {
            "name": "PROCESSED_PREFIX",
            "value": os.getenv(
                "PROCESSED_PREFIX",
                "processed"
            )
        },
        {
            "name": "AWS_DEFAULT_REGION",
            "value": AWS_REGION
        },
        {
            "name": "PYTHONUNBUFFERED",
            "value": "1"
        }
    ]

    if creds:

        frozen = creds.get_frozen_credentials()

        env_vars.extend(
            [
                {
                    "name": "AWS_ACCESS_KEY_ID",
                    "value": frozen.access_key
                },
                {
                    "name": "AWS_SECRET_ACCESS_KEY",
                    "value": frozen.secret_key
                }
            ]
        )

        if frozen.token:

            env_vars.append(
                {
                    "name": "AWS_SESSION_TOKEN",
                    "value": frozen.token
                }
            )

    container_def = {
        "name": CONTAINER_NAME,
        "image": image_uri,
        "essential": True,
        "environment": env_vars
    }

    if log_group_name:

        container_def["logConfiguration"] = {
            "logDriver": "awslogs",
            "options": {
                "awslogs-group": log_group_name,
                "awslogs-region": AWS_REGION,
                "awslogs-stream-prefix": "ausaem"
            }
        }

    res = ecs_client.register_task_definition(
        family=ECS_TASK_FAMILY,
        executionRoleArn=execution_role_arn,
        taskRoleArn=execution_role_arn,
        networkMode="awsvpc",
        containerDefinitions=[container_def],
        requiresCompatibilities=["FARGATE"],
        cpu=str(cpu),
        memory=str(memory)
    )

    task_def_arn = res[
        "taskDefinition"
    ]["taskDefinitionArn"]

    print(
        f"[+] Task Definition registered: "
        f"{task_def_arn}",
        flush=True
    )

    return task_def_arn


def ensure_ecs_cluster(
    ecs_client,
    cluster_name
):

    try:

        res = ecs_client.describe_clusters(
            clusters=[cluster_name]
        )

        existing = [
            c
            for c in res["clusters"]
            if c["status"] == "ACTIVE"
        ]

        if existing:
            return existing[0]["clusterArn"]

    except Exception:
        pass

    print(
        f"[*] Creating ECS Fargate Cluster "
        f"'{cluster_name}' in {AWS_REGION}...",
        flush=True
    )

    res = ecs_client.create_cluster(
        clusterName=cluster_name
    )

    return res["cluster"]["clusterArn"]


def get_default_vpc_subnets_and_security_group(
    ec2_client
):

    vpcs = ec2_client.describe_vpcs(
        Filters=[
            {
                "Name": "isDefault",
                "Values": ["true"]
            }
        ]
    )["Vpcs"]

    if not vpcs:
        vpcs = ec2_client.describe_vpcs()["Vpcs"]

    if not vpcs:
        raise RuntimeError(
            "No VPC found in current AWS region."
        )

    vpc_id = vpcs[0]["VpcId"]

    subnets = ec2_client.describe_subnets(
        Filters=[
            {
                "Name": "vpc-id",
                "Values": [vpc_id]
            }
        ]
    )["Subnets"]

    subnet_ids = [
        s["SubnetId"]
        for s in subnets
    ]

    if not subnet_ids:
        raise RuntimeError(
            f"No subnets found in VPC {vpc_id}."
        )

    # Get or create security group
    sec_groups = ec2_client.describe_security_groups(
        Filters=[
            {
                "Name": "group-name",
                "Values": ["ausaem-ecs-sg"]
            }
        ]
    )["SecurityGroups"]

    if sec_groups:

        sg_id = sec_groups[0]["GroupId"]

    else:

        try:

            sg = ec2_client.create_security_group(
                GroupName="ausaem-ecs-sg",
                Description=(
                    "Security group for "
                    "AusAEM ECS Fargate tasks"
                ),
                VpcId=vpc_id
            )

            sg_id = sg["GroupId"]

        except Exception:

            def_sgs = (
                ec2_client.describe_security_groups(
                    Filters=[
                        {
                            "Name": "vpc-id",
                            "Values": [vpc_id]
                        },
                        {
                            "Name": "group-name",
                            "Values": ["default"]
                        }
                    ]
                )["SecurityGroups"]
            )

            sg_id = def_sgs[0]["GroupId"]

    return subnet_ids, [sg_id]


def run_fargate_task(
    ecs_client,
    cluster_name,
    task_def_arn,
    subnet_ids,
    sg_ids,
    cmd_args,
    survey="",
    s3_bucket=S3_BUCKET
):

    print(
        "\n[4/4] Launching Task on AWS ECS Fargate...",
        flush=True
    )

    overrides = {
        "containerOverrides": [
            {
                "name": CONTAINER_NAME,
                "command": cmd_args
            }
        ]
    }

    network_config = {
        "awsvpcConfiguration": {
            "subnets": subnet_ids,
            "securityGroups": sg_ids,
            "assignPublicIp": "ENABLED"
        }
    }

    res = ecs_client.run_task(
        cluster=cluster_name,
        taskDefinition=task_def_arn,
        launchType="FARGATE",
        count=1,
        platformVersion="LATEST",
        networkConfiguration=network_config,
        overrides=overrides
    )

    tasks = res.get("tasks", [])

    if not tasks:

        failures = res.get(
            "failures",
            []
        )

        raise RuntimeError(
            f"Failed to launch ECS task: {failures}"
        )

    task_arn = tasks[0]["taskArn"]
    task_id = task_arn.split("/")[-1]

    print(
        "===========================================================================",
        flush=True
    )

    print(
        "   AWS ECS FARGATE TASK LAUNCHED SUCCESSFULLY!",
        flush=True
    )

    print(
        "===========================================================================",
        flush=True
    )

    print(f"  Task ARN:        {task_arn}")
    print(f"  Task ID:         {task_id}")
    print(f"  Cluster:         {cluster_name}")
    print(f"  Target Survey:   {survey or 'ALL'}")
    print(f"  S3 Destination:  s3://{s3_bucket}/")
    print(
        "---------------------------------------------------------------------------",
        flush=True
    )

    print(
        "  [OK] The task is now running in AWS Cloud."
    )

    print(
        "  [OK] YOU CAN SAFELY SHUT DOWN YOUR COMPUTER NOW!"
    )

    print(
        f"  [LOGS] View live logs in AWS CloudWatch: "
        f"/ecs/{ECS_TASK_FAMILY}"
    )

    print(
        "===========================================================================",
        flush=True
    )

    return task_arn, task_id


def monitor_fargate_task(
    ecs_client,
    cluster_name,
    task_id,
    timeout_minutes=180
):

    print(
        f"\n[*] Monitoring AWS ECS Fargate task lifecycle "
        f"({task_id})...\n",
        flush=True
    )

    t_deadline = time.time() + (
        timeout_minutes * 60
    )

    last_status = None

    while time.time() < t_deadline:

        try:

            res = ecs_client.describe_tasks(
                cluster=cluster_name,
                tasks=[task_id]
            )

            if res.get("tasks"):

                task = res["tasks"][0]

                status = task.get(
                    "lastStatus"
                )

                desired = task.get(
                    "desiredStatus"
                )

                if status != last_status:

                    print(
                        f"      Status: {status} "
                        f"(Desired: {desired})",
                        flush=True
                    )

                    last_status = status

                if status == "STOPPED":

                    containers = task.get(
                        "containers",
                        []
                    )

                    exit_code = (
                        containers[0].get(
                            "exitCode",
                            0
                        )
                        if containers
                        else 0
                    )

                    reason = task.get(
                        "stoppedReason",
                        ""
                    )

                    if exit_code == 0:

                        print(
                            "\n[+] ECS Fargate Task "
                            "completed successfully!"
                        )

                    else:

                        print(
                            f"\n[!] ECS Fargate Task stopped "
                            f"with exit code {exit_code}. "
                            f"Reason: {reason}"
                        )

                    return exit_code

        except Exception:
            pass

        time.sleep(5)

    print(
        "\n[!] Monitoring timeout reached."
    )

    return None


def main():

    parser = argparse.ArgumentParser(
        description=(
            "Deploy and run AusAEM-WA "
            "processing on AWS ECS Fargate"
        )
    )

    parser.add_argument(
        "-s",
        "--survey",
        default="",
        help=(
            "Survey name. Example: "
            "Earaheedy. Empty runs all surveys."
        )
    )

    parser.add_argument(
        "--output-dir",
        default="AusAEM_WA_EM_Data",
        help="Output directory"
    )

    parser.add_argument(
        "--temp-dir",
        default=None,
        help="Temporary working directory"
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Force reprocessing"
    )

    parser.add_argument(
        "--bucket",
        default=S3_BUCKET,
        help=f"Target S3 bucket (default: {S3_BUCKET})"
    )

    parser.add_argument(
        "--s3-prefix",
        default=os.getenv(
            "RAW_PREFIX",
            "raw"
        ),
        help="S3 prefix"
    )

    parser.add_argument(
        "--skip-build",
        action="store_true",
        help=(
            "Skip Docker build/push and "
            "use existing image in ECR"
        )
    )

    parser.add_argument(
        "--monitor",
        action="store_true",
        help=(
            "Monitor task status in terminal "
            "(default: detached)"
        )
    )

    args = parser.parse_args()

    sts_client = boto3.client(
        "sts",
        region_name=AWS_REGION
    )

    ecr_client = boto3.client(
        "ecr",
        region_name=AWS_REGION
    )

    ecs_client = boto3.client(
        "ecs",
        region_name=AWS_REGION
    )

    logs_client = boto3.client(
        "logs",
        region_name=AWS_REGION
    )

    ec2_client = boto3.client(
        "ec2",
        region_name=AWS_REGION
    )

    account_id = get_account_id(
        sts_client
    )

    print(
        f"Connected to AWS Account: "
        f"{account_id} | Region: {AWS_REGION}",
        flush=True
    )

    ecr_uri = ensure_ecr_repo(
        ecr_client,
        ECR_REPO_NAME
    )

    if not args.skip_build:

        image_uri = build_and_push_docker(
            ecr_uri,
            tag="latest"
        )

    else:

        image_uri = f"{ecr_uri}:latest"

        print(
            "[*] Skipping Docker build. "
            f"Using existing image in ECR: {image_uri}",
            flush=True
        )

    execution_role_arn = (
        get_iam_execution_role_arn(
            account_id
        )
    )

    log_group_name = (
        f"/ecs/{ECS_TASK_FAMILY}"
    )

    ensure_cloudwatch_log_group(
        logs_client,
        log_group_name
    )

    task_def_arn = register_task_definition(
        ecs_client,
        execution_role_arn,
        image_uri,
        log_group_name=log_group_name,
        memory="8192",
        cpu="2048"
    )

    cluster_arn = ensure_ecs_cluster(
        ecs_client,
        ECS_CLUSTER_NAME
    )

    subnet_ids, sg_ids = (
        get_default_vpc_subnets_and_security_group(
            ec2_client
        )
    )

    # Build container command
    cmd_args = [
        "ausem.py"
    ]

    if args.survey:
        cmd_args.extend(
            [
                "--survey",
                args.survey
            ]
        )

    if args.output_dir:
        cmd_args.extend(
            [
                "--output-dir",
                args.output_dir
            ]
        )

    if args.temp_dir:
        cmd_args.extend(
            [
                "--temp-dir",
                args.temp_dir
            ]
        )

    if args.force:
        cmd_args.append("--force")

    task_arn, task_id = run_fargate_task(
        ecs_client,
        ECS_CLUSTER_NAME,
        task_def_arn,
        subnet_ids,
        sg_ids,
        cmd_args=cmd_args,
        survey=args.survey,
        s3_bucket=args.bucket
    )

    if args.monitor:

        monitor_fargate_task(
            ecs_client,
            ECS_CLUSTER_NAME,
            task_id
        )


if __name__ == "__main__":
    main()
