"""
================================================================================
AusAEM-WA AWS ECS Fargate Cloud Worker
================================================================================

Headless & interactive entrypoint for executing AusAEM-WA airborne
electromagnetic (AEM) data processing in AWS Cloud
(ECS Fargate / Docker / CLI).

Processes official AusAEM-WA survey data and organizes the resulting
CSV and metadata files for cloud-based execution.

Configured survey areas:
  ├── Earaheedy
  ├── Eastern_Goldfields
  ├── East_Yilgarn_Albany_Fraser
  ├── Murchison
  ├── South_West_Albany
  ├── Western_Resources_Corridor
  └── Northern_WA

Output structure:
  AusAEM_WA_EM_Data/
    ├── <survey>/
    │   ├── <survey>.csv
    │   ├── <survey>.dfn.txt
    │   └── <survey>.des.txt

Supports:
  - AWS ECS Fargate
  - Docker
  - Local CLI execution
  - Single survey processing
  - Full survey processing
  - Force reprocessing
  - Custom output and temporary directories
"""

import os
import sys
import time
import argparse

# Ensure current directory is in sys.path
sys.path.insert(
    0,
    os.path.dirname(os.path.abspath(__file__))
)

# Import the main AusAEM processing pipeline
from ausaem import main as run_ausaem


def parse_arguments():
    default_output_dir = os.getenv(
        "OUTPUT_DIR",
        "AusAEM_WA_EM_Data"
    )

    default_temp_dir = os.getenv(
        "TEMP_DIR",
        "temp_work"
    )

    parser = argparse.ArgumentParser(
        description=(
            "AusAEM-WA AWS ECS Fargate Cloud Worker - "
            "Airborne Electromagnetic Data Processing"
        )
    )

    parser.add_argument(
        "-s",
        "--survey",
        default=os.getenv("SURVEY_NAME"),
        help=(
            "AusAEM-WA survey name to process. "
            "If omitted, all configured surveys are processed."
        ),
    )

    parser.add_argument(
        "-o",
        "--output-dir",
        default=default_output_dir,
        help=(
            f"Output directory for processed AusAEM-WA data "
            f"(default: {default_output_dir})"
        ),
    )

    parser.add_argument(
        "--temp-dir",
        default=default_temp_dir,
        help=(
            f"Temporary working directory "
            f"(default: {default_temp_dir})"
        ),
    )

    parser.add_argument(
        "--force",
        action="store_true",
        default=os.getenv(
            "OVERWRITE",
            "false"
        ).lower() in ("true", "1", "yes"),
        help=(
            "Force re-download and reprocessing even when "
            "completed survey outputs already exist"
        ),
    )

    parser.add_argument(
        "--bucket",
        default=os.getenv(
            "S3_BUCKET",
            "ausaem-wa-data"
        ),
        help=(
            "Target AWS S3 bucket. "
            "Used for cloud deployment configuration."
        ),
    )

    parser.add_argument(
        "--s3-prefix",
        default=os.getenv(
            "RAW_PREFIX",
            "raw"
        ),
        help=(
            "S3 key prefix for AusAEM-WA data."
        ),
    )

    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Run in interactive mode",
    )

    return parser.parse_args()


def build_ausaem_arguments(args):
    """
    Convert worker arguments into the command-line style arguments
    expected by ausaem.py.
    """

    ausaem_args = []

    if args.survey:
        ausaem_args.extend([
            "--survey",
            args.survey,
        ])

    if args.output_dir:
        ausaem_args.extend([
            "--output-dir",
            args.output_dir,
        ])

    if args.temp_dir:
        ausaem_args.extend([
            "--temp-dir",
            args.temp_dir,
        ])

    if args.force:
        ausaem_args.append("--force")

    return ausaem_args


def main():
    args = parse_arguments()

    print("=" * 75, flush=True)
    print(
        "      AusAEM-WA AWS ECS FARGATE CLOUD WORKER",
        flush=True
    )
    print(
        "      Airborne Electromagnetic Data Processing",
        flush=True
    )
    print("=" * 75, flush=True)

    print("\n[WORKER CONFIGURATION]", flush=True)

    print(
        f"  Survey:             "
        f"{args.survey if args.survey else 'ALL SURVEYS'}",
        flush=True
    )

    print(
        f"  Output Directory:   {args.output_dir}",
        flush=True
    )

    print(
        f"  Temporary Directory: {args.temp_dir}",
        flush=True
    )

    print(
        f"  S3 Bucket:          {args.bucket}",
        flush=True
    )

    print(
        f"  S3 Prefix:          {args.s3_prefix}",
        flush=True
    )

    print(
        f"  Force Processing:   {args.force}",
        flush=True
    )

    ausaem_args = build_ausaem_arguments(args)

    print(
        "\n[STARTING AusAEM-WA PROCESSING]",
        flush=True
    )

    t0 = time.time()

    # Save the original command-line arguments.
    original_argv = sys.argv

    try:
        # Run ausaem.py using the worker-generated arguments.
        sys.argv = [
            "ausaem.py",
            *ausaem_args,
        ]

        run_ausaem()

    finally:
        # Restore original command-line arguments.
        sys.argv = original_argv

    elapsed = time.time() - t0

    print(
        "\n" + "=" * 75,
        flush=True
    )

    print(
        "              AusAEM-WA PROCESSING SUMMARY",
        flush=True
    )

    print(
        "=" * 75,
        flush=True
    )

    if args.survey:
        print(
            f"Survey Processed:    {args.survey}",
            flush=True
        )
    else:
        print(
            "Surveys Processed:   All configured surveys",
            flush=True
        )

    print(
        f"Output Directory:    {args.output_dir}",
        flush=True
    )

    print(
        f"Execution Duration:  "
        f"{elapsed:.1f}s ({elapsed / 60:.1f} min)",
        flush=True
    )

    print(
        "=" * 75 + "\n",
        flush=True
    )


if __name__ == "__main__":
    try:
        main()

    except Exception as exc:
        import traceback

        err_msg = traceback.format_exc()

        print(
            f"\n[FATAL ERROR IN AusAEM-WA WORKER]:\n{err_msg}",
            file=sys.stderr,
            flush=True,
        )

        # Attempt to write the error into the configured temporary
        # working directory for cloud/container diagnostics.
        try:
            temp_dir = os.getenv(
                "TEMP_DIR",
                "temp_work"
            )

            os.makedirs(
                temp_dir,
                exist_ok=True
            )

            error_log = os.path.join(
                temp_dir,
                "worker_error.log"
            )

            with open(
                error_log,
                "w",
                encoding="utf-8"
            ) as f:
                f.write(err_msg)

            print(
                f"[ERROR LOG] {error_log}",
                file=sys.stderr,
                flush=True,
            )

        except Exception:
            pass

        sys.exit(1)
