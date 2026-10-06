import subprocess
import sys

def main():
    print("Starting AusAEM-WA pipeline...")

    result = subprocess.run(
        [sys.executable, "ausem.py"],
        check=False
    )

    if result.returncode != 0:
        print("AusAEM processing failed.")
        sys.exit(result.returncode)

    print("AusAEM processing completed successfully.")
    print("Starting S3 upload...")

    upload_result = subprocess.run(
        [sys.executable, "s3_uploader.py"],
        check=False
    )

    if upload_result.returncode != 0:
        print("S3 upload failed.")
        sys.exit(upload_result.returncode)

    print("S3 upload completed successfully.")

if __name__ == "__main__":
    main()
