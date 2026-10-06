import os
import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig


# Local folder containing the final AusAEM data
LOCAL_FOLDER = r"AusAEM_WA_EM_Data"

# These will come from your environment variables
BUCKET_NAME = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "AusAEM_WA_EM_Data/")


# AWS connection
s3 = boto3.client(
    "s3",
    config=Config(
        retries={
            "max_attempts": 10,
            "mode": "adaptive"
        }
    )
)

# Multipart upload for large files
transfer_config = TransferConfig(
    multipart_threshold=64 * 1024 * 1024,
    multipart_chunksize=64 * 1024 * 1024,
    max_concurrency=4,
    use_threads=True
)


def upload_folder():
    if not BUCKET_NAME:
        raise ValueError("S3_BUCKET is not configured.")

    if not os.path.exists(LOCAL_FOLDER):
        raise FileNotFoundError(
            f"Folder not found: {LOCAL_FOLDER}"
        )

    for root, dirs, files in os.walk(LOCAL_FOLDER):

        for filename in files:

            local_path = os.path.join(root, filename)

            relative_path = os.path.relpath(
                local_path,
                LOCAL_FOLDER
            )

            s3_key = (
                S3_PREFIX.rstrip("/")
                + "/"
                + relative_path.replace("\\", "/")
            )

            print(f"Uploading: {relative_path}")

            s3.upload_file(
                local_path,
                BUCKET_NAME,
                s3_key,
                Config=transfer_config
            )

            print("SUCCESS")


if __name__ == "__main__":
    upload_folder()
