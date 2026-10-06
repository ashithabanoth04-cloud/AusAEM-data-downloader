"""
================================================================================
S3 Uploader Module - AWS S3 Ingestion for AusAEM-WA Data
================================================================================

Handles cloud-native ingestion into AWS S3:
- Robust S3 credential resolution (.env, IAM Task Role, AWS CLI).
- Idempotency checks using head_object.
- In-memory byte uploads.
- Multipart upload support for large files.
- JSON metadata read/write directly in S3.
"""

import os
import io
import json
import time
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from typing import Optional, Dict, Any, Union


# Automatically load environment variables from .env if present
try:
    from dotenv import load_dotenv

    load_dotenv()

    script_dir = os.path.dirname(
        os.path.abspath(__file__)
    )

    load_dotenv(
        os.path.join(script_dir, ".env")
    )

except ImportError:
    pass


_s3_client = None


def get_s3_client(
    region_name: Optional[str] = None
):
    """
    Returns a cached boto3 S3 client using standard AWS
    credential resolution.
    """

    global _s3_client

    if _s3_client is None:

        aws_region = (
            region_name
            or os.getenv("AWS_DEFAULT_REGION")
            or os.getenv("AWS_REGION")
            or "ap-south-1"
        )

        access_key = os.getenv(
            "AWS_ACCESS_KEY_ID"
        )

        secret_key = os.getenv(
            "AWS_SECRET_ACCESS_KEY"
        )

        if access_key and secret_key:

            _s3_client = boto3.client(
                "s3",
                region_name=aws_region,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )

        else:

            _s3_client = boto3.client(
                "s3",
                region_name=aws_region
            )

    return _s3_client


def ensure_bucket_exists(
    bucket: str,
    region_name: Optional[str] = None,
    s3_client=None
) -> str:
    """
    Checks if an S3 bucket exists and creates it if required.
    """

    if s3_client is None:
        s3_client = get_s3_client(
            region_name
        )

    region = (
        region_name
        or os.getenv("AWS_DEFAULT_REGION")
        or "ap-south-1"
    )

    try:

        s3_client.head_bucket(
            Bucket=bucket
        )

        return bucket

    except ClientError as e:

        error_code = str(
            e.response
            .get("Error", {})
            .get("Code", "")
        )

        if error_code in (
            "404",
            "NoSuchBucket"
        ):

            print(
                f"[*] Creating S3 Bucket "
                f"'{bucket}' in region {region}..."
            )

            if region == "us-east-1":

                s3_client.create_bucket(
                    Bucket=bucket
                )

            else:

                s3_client.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={
                        "LocationConstraint": region
                    }
                )

            print(
                f"[+] S3 Bucket '{bucket}' "
                f"successfully created."
            )

            return bucket

        raise


def check_object_exists(
    bucket: str,
    key: str,
    s3_client=None
) -> bool:
    """
    Checks whether an S3 object already exists.
    """

    if s3_client is None:
        s3_client = get_s3_client()

    try:

        s3_client.head_object(
            Bucket=bucket,
            Key=key
        )

        return True

    except ClientError as e:

        error_code = str(
            e.response
            .get("Error", {})
            .get("Code", "")
        )

        if error_code in (
            "404",
            "NoSuchKey",
            "NotFound"
        ):

            return False

        raise

    except Exception as e:

        if any(
            x in str(e)
            for x in (
                "404",
                "NoSuchKey",
                "NotFound"
            )
        ):

            return False

        raise


def read_s3_json(
    bucket: str,
    key: str,
    s3_client=None
) -> Optional[Dict[str, Any]]:
    """
    Reads and parses a JSON document directly from S3.
    """

    if s3_client is None:
        s3_client = get_s3_client()

    try:

        response = s3_client.get_object(
            Bucket=bucket,
            Key=key
        )

        content = (
            response["Body"]
            .read()
            .decode("utf-8")
        )

        return json.loads(content)

    except ClientError as e:

        error_code = str(
            e.response
            .get("Error", {})
            .get("Code", "")
        )

        if error_code in (
            "404",
            "NoSuchKey",
            "NotFound"
        ):

            return None

        raise

    except Exception:
        return None


def upload_bytes_to_s3(
    data: Union[
        bytes,
        io.BytesIO,
        str
    ],
    bucket: str,
    key: str,
    content_type: str = "text/csv",
    s3_client=None
) -> str:
    """
    Uploads in-memory data directly to S3.
    """

    if s3_client is None:
        s3_client = get_s3_client()

    if isinstance(data, io.BytesIO):

        body = data.getvalue()

    elif isinstance(data, bytes):

        body = data

    elif isinstance(data, str):

        body = data.encode("utf-8")

    else:

        raise TypeError(
            "Unsupported data type for S3 "
            f"byte upload: {type(data)}"
        )

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type
    )

    return (
        f"s3://{bucket}/{key}"
    )


def upload_fileobj_to_s3(
    fileobj,
    bucket: str,
    key: str,
    content_type: Optional[str] = None,
    s3_client=None
) -> str:
    """
    Streams a file-like object to S3 using multipart upload.
    """

    if (
        fileobj is None
        or not bucket
        or not key
    ):

        raise ValueError(
            "fileobj, bucket, and key are required"
        )

    if s3_client is None:
        s3_client = get_s3_client()

    if content_type is None:

        if key.endswith(".csv"):

            content_type = "text/csv"

        elif key.endswith(".json"):

            content_type = "application/json"

        elif key.endswith(".zip"):

            content_type = "application/zip"

        else:

            content_type = (
                "application/octet-stream"
            )

    transfer_config = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,
        max_concurrency=4,
        multipart_chunksize=8 * 1024 * 1024,
        use_threads=True,
    )

    extra_args = {
        "ContentType": content_type
    }

    for attempt in range(1, 6):

        try:

            if hasattr(fileobj, "seek"):

                try:
                    fileobj.seek(0)
                except Exception:
                    pass

            s3_client.upload_fileobj(
                Fileobj=fileobj,
                Bucket=bucket,
                Key=key,
                ExtraArgs=extra_args,
                Config=transfer_config,
            )

            return (
                f"s3://{bucket}/{key}"
            )

        except Exception as e:

            if attempt < 5:

                print(
                    f"      [!] S3 upload transient "
                    f"network error on {key} "
                    f"(attempt {attempt}/5): {e}. "
                    f"Retrying in {attempt * 3}s..."
                )

                time.sleep(
                    attempt * 3
                )

            else:

                raise

    return (
        f"s3://{bucket}/{key}"
    )


def upload_file_to_s3(
    file_path: str,
    bucket: str,
    key: str,
    content_type: Optional[str] = None,
    s3_client=None
) -> str:
    """
    Uploads a local file to S3 using multipart upload.
    """

    if (
        not file_path
        or not bucket
        or not key
    ):

        raise ValueError(
            "file_path, bucket, and key are required"
        )

    with open(
        file_path,
        "rb"
    ) as f:

        return upload_fileobj_to_s3(
            fileobj=f,
            bucket=bucket,
            key=key,
            content_type=content_type,
            s3_client=s3_client
        )
