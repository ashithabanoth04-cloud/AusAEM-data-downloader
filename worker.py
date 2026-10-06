import os
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


if __name__ == "__main__":
    main()
