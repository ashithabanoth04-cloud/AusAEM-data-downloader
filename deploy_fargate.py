import os
import sys
import time
import base64
import subprocess
import argparse

import boto3
from botocore.exceptions import ClientError


# Load .env if python-dotenv is available
try:
    from dotenv import load_dotenv

    load_dotenv()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    load_dotenv(os.path.join(script_dir, ".env"))
except ImportError:
    pass


# AWS / project configuration
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")

ECR_REPO_NAME = os.getenv(
    "ECR_REPO_NAME",
    "ausaem-wa-worker"
)

ECS_CLUSTER_NAME = os.getenv(
    "ECS_CLUSTER_NAME",
    "ausaem-fargate-cluster"
)

ECS_TASK_FAMILY = os.getenv(
    "ECS_TASK_FAMILY",
    "ausaem-task"
)

S3_BUCKET = os.getenv("S3_BUCKET", "")

S3_PREFIX = os.getenv(
    "S3_PREFIX",
    "AusAEM_WA_EM_Data"
)

CONTAINER_NAME = "ausaem-worker-container"


def get_account_id(sts_client):
    return sts_client.get_caller_identity()["Account"]


def ensure_ecr_repo(ecr_client, repo_name):
    try:
        response = ecr_client.describe_repositories(
            repositoryNames=[repo_name]
        )

        return response["repositories"][0]["repositoryUri"]

    except ClientError as e:

        if e.response["Error"]["Code"] == "RepositoryNotFoundException":

            print(
                f"[*] Creating ECR repository '{repo_name}'..."
            )

            response = ecr_client.create_repository(
                repositoryName=repo_name
            )

            return response["repository"]["repositoryUri"]

        raise


def get_ecr_login_command(ecr_client):
    token_response = ecr_client.get_authorization_token()

    auth_data = token_response["authorizationData"][0]

    auth_token = base64.b64decode(
        auth_data["authorizationToken"]
    ).decode("utf-8")

    username, password = auth_token.split(":")

    endpoint = auth_data["proxyEndpoint"]

    return username, password, endpoint


def build_and_push_docker(ecr_uri, tag="latest"):

    print(
        "\n[1/4] Building Docker image for Linux/AMD64...",
        flush=True
    )

    full_image_uri = f"{ecr_uri}:{tag}"

    script_dir = os.path.dirname(
        os.path.abspath(__file__)
    )

    build_command = [
        "docker",
        "build",
        "--platform",
        "linux/amd64",
        "-t",
        full_image_uri,
        script_dir
    ]

    subprocess.run(build_command, check=True)

    print(
        "\n[2/4] Logging in to AWS ECR...",
        flush=True
    )

    ecr_client = boto3.client(
        "ecr",
        region_name=AWS_REGION
    )

    username, password, endpoint = get_ecr_login_command(
        ecr_client
    )

    login_process = subprocess.Popen(
        [
            "docker",
            "login",
            "--username",
            username,
            "--password-stdin",
            endpoint
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    stdout, stderr = login_process.communicate(
        input=password
    )

    if login_process.returncode != 0:
        raise RuntimeError(
            f"Docker login failed: {stderr}"
        )

    print("[+] Docker login successful.")

    push_command = [
        "docker",
        "push",
        full_image_uri
    ]

    max_attempts = 10

    for attempt in range(1, max_attempts + 1):

        try:

            print(
                f"[*] Pushing image "
                f"(attempt {attempt}/{max_attempts})...",
                flush=True
            )

            subprocess.run(
                push_command,
                check=True
            )

            print(
                f"[+] Image pushed successfully: "
                f"{full_image_uri}"
            )

            return full_image_uri

        except subprocess.CalledProcessError:

            if attempt < max_attempts:

                print(
                    "[!] Push failed temporarily. "
                    "Retrying in 3 seconds..."
                )

                time.sleep(3)

            else:

                raise RuntimeError(
                    "Docker push failed after "
                    f"{max_attempts} attempts."
                )


def get_iam_execution_role_arn(account_id):

    return (
        f"arn:aws:iam::{account_id}:role/"
        "ecsTaskExecutionRole"
    )


def ensure_cloudwatch_log_group(
    logs_client,
    log_group_name
):

    try:

        logs_client.create_log_group(
            logGroupName=log_group_name
        )

        print(
            f"[+] Created CloudWatch log group: "
            f"{log_group_name}"
        )

    except logs_client.exceptions.ResourceAlreadyExistsException:

        pass

    except Exception as e:

        print(
            f"[!] Could not create log group: {e}"
        )


def register_task_definition(
    ecs_client,
    execution_role_arn,
    image_uri,
    log_group_name,
    memory="8192",
    cpu="2048"
):

    print(
        "\n[3/4] Registering ECS Fargate task definition...",
        flush=True
    )

    environment = [
        {
        "name": "S3_BUCKET",
        "value": "spectropy-processed-data"
        },
        {
            "name": "S3_PREFIX",
            "value": S3_PREFIX
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

    container_definition = {
        "name": CONTAINER_NAME,
        "image": image_uri,
        "essential": True,
        "environment": environment,
        "logConfiguration": {
            "logDriver": "awslogs",
            "options": {
                "awslogs-group": log_group_name,
                "awslogs-region": AWS_REGION,
                "awslogs-stream-prefix": "ausaem"
            }
        }
    }

    response = ecs_client.register_task_definition(

        family=ECS_TASK_FAMILY,

        executionRoleArn=execution_role_arn,

        containerDefinitions=[
            container_definition
        ],

        networkMode="awsvpc",

        requiresCompatibilities=[
            "FARGATE"
        ],

        cpu=str(cpu),

        memory=str(memory)
    )

    task_definition_arn = (
        response["taskDefinition"]
        ["taskDefinitionArn"]
    )

    print(
        f"[+] Task definition registered: "
        f"{task_definition_arn}"
    )

    return task_definition_arn


def ensure_ecs_cluster(
    ecs_client,
    cluster_name
):

    try:

        response = ecs_client.describe_clusters(
            clusters=[cluster_name]
        )

        active_clusters = [
            cluster
            for cluster in response["clusters"]
            if cluster["status"] == "ACTIVE"
        ]

        if active_clusters:
            return active_clusters[0]["clusterArn"]

    except Exception:
        pass

    print(
        f"[*] Creating ECS cluster '{cluster_name}'..."
    )

    response = ecs_client.create_cluster(
        clusterName=cluster_name
    )

    return response["cluster"]["clusterArn"]


def get_default_vpc_network(
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
            "No VPC found in AWS region."
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
        subnet["SubnetId"]
        for subnet in subnets
    ]

    if not subnet_ids:

        raise RuntimeError(
            f"No subnets found in VPC {vpc_id}."
        )

    security_groups = ec2_client.describe_security_groups(
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

    if not security_groups:

        raise RuntimeError(
            "No default security group found."
        )

    security_group_id = (
        security_groups[0]["GroupId"]
    )

    return subnet_ids, [security_group_id]


def run_fargate_task(
    ecs_client,
    cluster_name,
    task_definition_arn,
    subnet_ids,
    security_group_ids
):

    print(
        "\n[4/4] Launching AusAEM task on ECS Fargate...",
        flush=True
    )

    network_configuration = {
        "awsvpcConfiguration": {
            "subnets": subnet_ids,
            "securityGroups": security_group_ids,
            "assignPublicIp": "ENABLED"
        }
    }

    response = ecs_client.run_task(

        cluster=cluster_name,

        taskDefinition=task_definition_arn,

        launchType="FARGATE",

        count=1,

        platformVersion="LATEST",

        networkConfiguration=network_configuration
    )

    tasks = response.get("tasks", [])

    if not tasks:

        failures = response.get(
            "failures",
            []
        )

        raise RuntimeError(
            f"Failed to launch Fargate task: "
            f"{failures}"
        )

    task_arn = tasks[0]["taskArn"]

    task_id = task_arn.split("/")[-1]

    print()
    print("=" * 70)
    print(" AUS AEM ECS FARGATE TASK STARTED")
    print("=" * 70)
    print(f"Task ARN:       {task_arn}")
    print(f"Task ID:        {task_id}")
    print(f"Cluster:        {cluster_name}")
    print(f"S3 destination: s3://{S3_BUCKET}/{S3_PREFIX}/")
    print()
    print("The task is now running in AWS.")
    print("=" * 70)

    return task_arn, task_id


def main():

    global S3_BUCKET

    parser = argparse.ArgumentParser(
        description=(
            "Deploy AusAEM-WA processing "
            "to AWS ECS Fargate"
        )
    )

    parser.add_argument(
        "--bucket",
        default=S3_BUCKET,
        help="S3 bucket"
    )

    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Use existing Docker image in ECR"
    )

    args = parser.parse_args()

    bucket = args.bucket

    # Make sure the bucket supplied through --bucket
    # is the same value used in the ECS task definition.
    S3_BUCKET = bucket

    if not bucket:

        raise RuntimeError(
            "S3_BUCKET is not configured. "
            "Set it in .env or use --bucket."
        )

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
        f"Connected to AWS account: "
        f"{account_id}"
    )

    print(
        f"AWS region: {AWS_REGION}"
    )

    ecr_uri = ensure_ecr_repo(
        ecr_client,
        ECR_REPO_NAME
    )

    if not args.skip_build:

        image_uri = build_and_push_docker(
            ecr_uri
        )

    else:

        image_uri = (
            f"{ecr_uri}:latest"
        )

        print(
            f"[*] Using existing ECR image: "
            f"{image_uri}"
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

    task_definition_arn = (
        register_task_definition(
            ecs_client,
            execution_role_arn,
            image_uri,
            log_group_name
        )
    )

    ensure_ecs_cluster(
        ecs_client,
        ECS_CLUSTER_NAME
    )

    subnet_ids, security_group_ids = (
        get_default_vpc_network(
            ec2_client
        )
    )

    run_fargate_task(
        ecs_client,
        ECS_CLUSTER_NAME,
        task_definition_arn,
        subnet_ids,
        security_group_ids
    )


if __name__ == "__main__":
    main()
