#!/usr/bin/env python3
"""
AusAEM-WA Airborne Electromagnetic (AEM) Automated Downloader, Processor & AWS S3 Uploader
==========================================================================================
A fully automated, zero-configuration Python tool to download, locate, parse,
and convert official AusAEM-WA airborne electromagnetic datasets into clean,
standardized tabular CSV and readable metadata files, and automatically upload
the generated outputs to Amazon Web Services (AWS) S3.

Default Usage (Runs complete workflow automatically):
    python ausaem.py

Specific Survey Area:
    python ausaem.py --survey Earaheedy

Local-only Run (Bypass S3 Upload):
    python ausaem.py --no-s3

Force Re-download & Re-processing:
    python ausaem.py --force

Custom Directories / S3 Target:
    python ausaem.py --output-dir AusAEM_WA_EM_Data --temp-dir temp_work --bucket my-bucket --prefix processed_data/AusAEM_WA_EM_Data

Features:
- Fully automated: Running `python ausaem.py` with NO arguments automatically processes
  all seven AusAEM-WA survey areas from official GSWA / Geoscience Australia sources.
- Smart skip: Automatically detects already completed survey folders and verifies their
  integrity so completed datasets are preserved and not re-downloaded. If configured for
  S3, it checks and ensures remote objects are present without redundant work.
- Stream-based parser: Handles multi-gigabyte .dat files line-by-line without loading
  the entire dataset into memory.
- Dynamic schema extraction: Robustly interprets ASEG-GDF2 .dfn field definitions and
  expands multi-channel array columns (e.g. 15F12.6 -> Name[1]...Name[15]).
- Multi-block merger: Seamlessly combines multi-block surveys (e.g. Murchison Blocks E-H,
  South West Albany Blocks A-D) into a single unified CSV.
- Clean output structure: Keeps ONLY the final CSV, .dfn.txt, and .des.txt in the survey
  folders. All raw .dat files and temporary uncompressed files are removed.
- Seamless AWS S3 Integration: Automatically uploads final CSV, DFN.txt, and DES.txt to
  S3 using boto3 with multipart upload for large files and AES256 server-side encryption.
- Zero Hard-Coded Credentials: Strictly uses the standard AWS credential chain and
  environment variables (.env / AWS CLI / IAM). Never logs secret keys.
- Fault-tolerant: If one survey encounters a network, parsing, or S3 issue, it logs the error,
  records the status, preserves local files, and automatically continues to the remaining surveys.
- Detailed final summary table: Reports row counts, column counts, file sizes, local status,
  and S3 upload status for all seven survey areas.
"""

import os
import sys
import re
import time
import json
import shutil
import zipfile
import subprocess
import argparse
import urllib.request
import ssl
from datetime import datetime

# AWS SDK & Environment Configuration
import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# ==============================================================================
# OFFICIAL SURVEY REGISTRY & PRESETS
# ==============================================================================

OFFICIAL_SURVEYS = {
    "Earaheedy": {
        "registration": 71636,
        "name": "AusAEM20-WA Earaheedy TEMPEST",
        "system": "TEMPEST",
        "url": "https://geodownloads.dmp.wa.gov.au/downloads/geophysics/71636/71636_CGG_final_data.zip",
        "dat_patterns": [r"Earaheedy.*_Final_EM\.dat$", r".*Final_EM\.dat$"],
        "dfn_patterns": [r"Earaheedy.*_Final_EM\.dfn$", r".*Final_EM\.dfn$"],
        "des_patterns": [r"Earaheedy.*_Final_EM\.des$", r".*Final_EM\.des$"]
    },
    "Eastern_Goldfields": {
        "registration": 71635,
        "name": "AusAEM20-WA EGF TEMPEST",
        "system": "TEMPEST",
        "url": "https://geodownloads.dmp.wa.gov.au/downloads/geophysics/71635/71635_CGG_final_data.zip",
        "dat_patterns": [r"Eastern_Goldfields.*_Final_EM\.dat$", r".*Final_EM\.dat$"],
        "dfn_patterns": [r"Eastern_Goldfields.*_Final_EM\.dfn$", r".*Final_EM\.dfn$"],
        "des_patterns": [r"Eastern_Goldfields.*_Final_EM\.des$", r".*Final_EM\.des$"]
    },
    "East_Yilgarn_Albany_Fraser": {
        "registration": 71586,
        "name": "AusAEM20-WA EY-Fraser TEMPEST",
        "system": "TEMPEST",
        "url": "https://geodownloads.dmp.wa.gov.au/downloads/geophysics/71586/71586_CGG_final_data.zip",
        "dat_patterns": [r"East_Yilgarn.*_Final_EM\.dat$", r".*Final_EM\.dat$"],
        "dfn_patterns": [r"East_Yilgarn.*_Final_EM\.dfn$", r".*Final_EM\.dfn$"],
        "des_patterns": [r"East_Yilgarn.*_Final_EM\.des$", r".*Final_EM\.des$"]
    },
    "Murchison": {
        "registration": 72050,
        "name": "AusAEM20-WA Murchison 2021 SkyTEM",
        "system": "SkyTEM 312",
        "url": "https://geodownloads.dmp.wa.gov.au/downloads/geophysics/72050/GA_2_AusAEM_WA_Murchison_EM_data.zip",
        "dat_patterns": [r"AusAEM_WA_Block_[E-H]_EM\.dat$"],
        "dfn_patterns": [r"AusAEM_WA_Block_E_EM\.dfn$", r".*_EM\.dfn$"],
        "des_patterns": [r"AusAEM_WA_Block_E_EM\.des$", r".*_EM\.des$"]
    },
    "South_West_Albany": {
        "registration": 71588,
        "name": "AusAEM20-WA SW-Albany SkyTEM",
        "system": "SkyTEM 312",
        "url": "https://geodownloads.dmp.wa.gov.au/downloads/geophysics/71588/GA_2_AusAEM_WA_SW_Albany_EM_data.zip",
        "dat_patterns": [r"AusAEM_WA_EM_Block_[A-D]\.dat$"],
        "dfn_patterns": [r"AusAEM_WA_EM_Block_A\.dfn$", r".*Block_.*\.dfn$"],
        "des_patterns": [r"AusAEM_WA_EM_Block_A\.des$", r".*Block_.*\.des$"]
    },
    "Western_Resources_Corridor": {
        "registration": 72374,
        "name": "AusAEM Western Resources Corridor 2022",
        "system": "TEMPEST",
        "url": "https://geodownloads.dmp.wa.gov.au/downloads/geophysics/72374/72374_AusAEM_WRC_4_EM_data.zip",
        "dat_patterns": [r"AusAEM_Western_Resources_Corridor_EM\.dat$", r"Western_Resources_Corridor.*EM\.dat$", r".*Final_EM\.dat$"],
        "dfn_patterns": [r"AusAEM_Western_Resources_Corridor_EM\.dfn$", r"Western_Resources_Corridor.*EM\.dfn$", r".*Final_EM\.dfn$"],
        "des_patterns": [r"AusAEM_Western_Resources_Corridor_EM\.des$", r"Western_Resources_Corridor.*EM\.des$", r".*Final_EM\.des$"]
    },
    "Northern_WA": {
        "registration": 71388,
        "name": "AusAEM02 2019 (Northern WA - GA Project 1320)",
        "system": "TEMPEST",
        "url": "https://d28rz98at9flks.cloudfront.net/140156/AusAEM_02_EM_data.zip",
        "dat_patterns": [r"AusAEM_Year2_WA_NT.*Final_EM\.dat$"],
        "dfn_patterns": [r"AusAEM_Year2_WA_NT_Final_EM\.dfn$"],
        "des_patterns": [r"AusAEM_Year2_WA_NT_Final_EM\.des$"]
    }
}

# ==============================================================================
# DISK & PATH UTILITIES
# ==============================================================================

def get_default_temp_dir():
    """
    Selects the optimal scratch directory with the most available disk space.
    Prefers D:\\temp_work if D: drive exists and has ample free space.
    """
    if os.path.exists("D:\\"):
        try:
            total, used, free = shutil.disk_usage("D:\\")
            if free > 10 * 1024 * 1024 * 1024:  # > 10 GB
                d_temp = os.path.join("D:\\", "temp_work")
                os.makedirs(d_temp, exist_ok=True)
                return d_temp
        except Exception:
            pass
    fallback = os.path.abspath("temp_work")
    os.makedirs(fallback, exist_ok=True)
    return fallback


def is_survey_completed(survey_out_dir, survey_name):
    """
    Checks if a survey has already been successfully downloaded and converted.
    Validates presence and non-trivial file size of CSV, DFN.txt, and DES.txt.
    """
    csv_file = os.path.join(survey_out_dir, f"{survey_name}.csv")
    dfn_file = os.path.join(survey_out_dir, f"{survey_name}.dfn.txt")
    des_file = os.path.join(survey_out_dir, f"{survey_name}.des.txt")
    
    if os.path.exists(csv_file) and os.path.exists(dfn_file) and os.path.exists(des_file):
        if os.path.getsize(csv_file) > 10 * 1024 and os.path.getsize(dfn_file) > 100 and os.path.getsize(des_file) > 100:
            return True
    return False


# ==============================================================================
# SCHEMA & METADATA PARSERS
# ==============================================================================

def parse_dfn_file(dfn_filepath):
    """
    Parses an ASEG-GDF2 .dfn file and extracts the expanded column names.
    Expands array channels: e.g. 15F12.6 -> Name[1], Name[2], ..., Name[15].
    """
    columns = []
    field_metadata = []
    
    if not os.path.exists(dfn_filepath):
        raise FileNotFoundError(f"DFN file not found: {dfn_filepath}")
        
    with open(dfn_filepath, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("DEFN"):
                continue
            parts = line.split(";")
            for p in parts[1:]:
                p = p.strip()
                if not p:
                    continue
                m = re.match(r'^([A-Za-z0-9_]+):([A-Za-z0-9\.]+)(?::(.*))?$', p)
                if m:
                    fname, ffmt, extra = m.group(1), m.group(2), m.group(3)
                    if fname.upper() in ('RT', 'COMMENTS', 'END DEFN'):
                        continue
                    
                    arr_match = re.match(r'^(\d+)[A-Za-z]', ffmt)
                    if arr_match:
                        count = int(arr_match.group(1))
                        expanded = [f"{fname}[{i}]" for i in range(1, count + 1)]
                        columns.extend(expanded)
                    else:
                        columns.append(fname)
                        expanded = [fname]
                        
                    field_metadata.append({
                        'name': fname,
                        'format': ffmt,
                        'columns': expanded,
                        'details': extra.strip() if extra else ""
                    })
                    
    return columns, field_metadata


def convert_dfn_to_readable_txt(dfn_src, dfn_dest, columns):
    """
    Writes a readable .dfn.txt file that preserves original definitions
    and appends the explicit expanded CSV column mapping.
    """
    with open(dfn_src, "r", encoding="utf-8", errors="ignore") as f_in:
        content = f_in.read()
        
    with open(dfn_dest, "w", encoding="utf-8") as f_out:
        f_out.write(content)
        if not content.endswith("\n"):
            f_out.write("\n")
        f_out.write("\n" + "=" * 70 + "\n")
        f_out.write("EXPANDED CSV TABULAR COLUMN MAPPING\n")
        f_out.write(f"Total Columns: {len(columns)}\n")
        f_out.write("=" * 70 + "\n")
        for i, col in enumerate(columns, start=1):
            f_out.write(f"Column {i:>3}: {col}\n")


def convert_des_to_readable_txt(des_src, des_dest):
    """
    Copies and formats the ASEG-GDF2 .des file into a readable .des.txt.
    """
    with open(des_src, "r", encoding="utf-8", errors="ignore") as f_in:
        content = f_in.read()
        
    with open(des_dest, "w", encoding="utf-8") as f_out:
        f_out.write(content)


# ==============================================================================
# STREAMING .DAT TO CSV CONVERTER
# ==============================================================================

def convert_dat_files_to_csv(dat_filepaths, csv_dest, expected_columns, chunk_buffer_mb=8):
    """
    Streams through one or more ASEG-GDF2 .dat files and writes a unified CSV file.
    Does NOT load the full file into memory. Handles multi-gigabyte datasets efficiently.
    Reconstructs wrapped / multi-line records according to the ASEG-GDF2 column specification
    and gracefully handles standard ASCII EOF control characters (0x1A / SUB).
    """
    expected_col_count = len(expected_columns)
    total_records = 0
    t0 = time.time()
    
    # Write Header
    with open(csv_dest, "w", encoding="utf-8", newline="") as f_out:
        f_out.write(','.join(expected_columns) + '\n')
        
    write_threshold = chunk_buffer_mb * 1024 * 1024
    
    for file_idx, dat_path in enumerate(dat_filepaths, start=1):
        if not os.path.exists(dat_path):
            raise FileNotFoundError(f"Source .dat file not found: {dat_path}")
            
        file_size_mb = os.path.getsize(dat_path) / (1024 * 1024)
        print(f"  [{file_idx}/{len(dat_filepaths)}] Streaming: {os.path.basename(dat_path)} ({file_size_mb:.2f} MB)...")
        sys.stdout.flush()
        
        file_rows = 0
        write_buf = []
        buf_size = 0
        t_file_start = time.time()
        
        # Token accumulator for records wrapped across multiple physical lines
        current_tokens = []
        
        with open(dat_path, "r", encoding="utf-8", errors="ignore") as f_in, \
             open(csv_dest, "a", encoding="utf-8", newline="") as f_out:
                 
            for line_idx, line in enumerate(f_in, start=1):
                # Clean trailing newlines, whitespace, and control characters (EOF / null bytes)
                clean_line = line.rstrip('\r\n\x1a\x00 \t')
                
                # Check for ASCII EOF marker (0x1A / SUB)
                if '\x1a' in line and not clean_line:
                    # Standard DOS/CP-M/ASEG-GDF ASCII EOF reached
                    break
                    
                if not clean_line or clean_line.startswith("COMM"):
                    continue
                    
                tokens = clean_line.split()
                
                # If a previous line was wrapped (accumulating tokens)
                if current_tokens:
                    current_tokens.extend(tokens)
                    if len(current_tokens) == expected_col_count:
                        csv_line = ','.join(current_tokens) + '\n'
                        write_buf.append(csv_line)
                        buf_size += len(csv_line)
                        total_records += 1
                        file_rows += 1
                        current_tokens = []
                    elif len(current_tokens) > expected_col_count:
                        raise ValueError(
                            f"Column count overflow in {os.path.basename(dat_path)} at line {line_idx}: "
                            f"accumulated {len(current_tokens)} tokens, expected {expected_col_count}"
                        )
                    else:
                        # Continuation still in progress
                        continue
                else:
                    if len(tokens) == expected_col_count:
                        csv_line = ','.join(tokens) + '\n'
                        write_buf.append(csv_line)
                        buf_size += len(csv_line)
                        total_records += 1
                        file_rows += 1
                    elif len(tokens) < expected_col_count:
                        # Start of a wrapped multi-line record
                        current_tokens.extend(tokens)
                    else:
                        raise ValueError(
                            f"Column mismatch in {os.path.basename(dat_path)} at line {line_idx}: "
                            f"expected {expected_col_count}, got {len(tokens)}"
                        )
                
                if buf_size >= write_threshold:
                    f_out.write(''.join(write_buf))
                    write_buf = []
                    buf_size = 0
                    
            # Handle any remaining complete record at EOF
            if current_tokens:
                if len(current_tokens) == expected_col_count:
                    write_buf.append(','.join(current_tokens) + '\n')
                    total_records += 1
                    file_rows += 1
                    current_tokens = []
                else:
                    raise ValueError(
                        f"Incomplete record at EOF in {os.path.basename(dat_path)}: "
                        f"had {len(current_tokens)} tokens, expected {expected_col_count}"
                    )
                    
            if write_buf:
                f_out.write(''.join(write_buf))
                write_buf = []
                
        file_elapsed = time.time() - t_file_start
        speed = file_rows / max(0.01, file_elapsed)
        print(f"      -> Streamed {file_rows:,} records in {file_elapsed:.1f}s ({speed:,.0f} rows/s)")
        
    total_elapsed = time.time() - t0
    final_size_mb = os.path.getsize(csv_dest) / (1024 * 1024)
    print(f"  [CSV Complete] Total Records: {total_records:,} | Size: {final_size_mb:.2f} MB | Elapsed: {total_elapsed:.1f}s")
    return total_records


# ==============================================================================
# DOWNLOAD & EXTRACTION UTILITIES
# ==============================================================================

def download_file_with_resume(url, dest_path):
    """
    Downloads a remote file with progress reporting and automatic resume support.
    Uses curl if available on the system; otherwise falls back to urllib.
    """
    print(f"  Source URL: {url}")
    print(f"  Target File: {dest_path}")
    sys.stdout.flush()
    
    # Try using curl with built-in retry and resume capabilities
    curl_bin = shutil.which("curl") or shutil.which("curl.exe")
    if curl_bin:
        cmd = [
            curl_bin, "-k", "-L",
            "--retry", "5",
            "--retry-delay", "3",
            "-C", "-",
            "-o", dest_path,
            url
        ]
        t0 = time.time()
        ret = subprocess.run(cmd)
        if ret.returncode == 0 and os.path.exists(dest_path) and os.path.getsize(dest_path) > 1024:
            size_mb = os.path.getsize(dest_path) / (1024 * 1024)
            print(f"  Download successful via curl ({size_mb:.2f} MB in {time.time()-t0:.1f}s).")
            return
            
    # Fallback to Python urllib
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    req = urllib.request.Request(url, headers=headers)
    
    t0 = time.time()
    with urllib.request.urlopen(req, context=ctx, timeout=60) as resp, open(dest_path, "wb") as f_out:
        total_len = resp.headers.get("Content-Length")
        total_bytes = int(total_len) if total_len else None
        downloaded = 0
        block_size = 1024 * 1024  # 1 MB
        
        while True:
            chunk = resp.read(block_size)
            if not chunk:
                break
            f_out.write(chunk)
            downloaded += len(chunk)
            elapsed = time.time() - t0
            speed_mb = (downloaded / (1024 * 1024)) / max(0.1, elapsed)
            if total_bytes:
                pct = (downloaded / total_bytes) * 100
                print(f"\r    ... Downloaded {downloaded/(1024*1024):.1f}/{total_bytes/(1024*1024):.1f} MB ({pct:.1f}%) [{speed_mb:.2f} MB/s]", end="")
            else:
                print(f"\r    ... Downloaded {downloaded/(1024*1024):.1f} MB [{speed_mb:.2f} MB/s]", end="")
            sys.stdout.flush()
    print("\n  Download finished successfully.")


def extract_matched_files_from_zip(zip_path, extract_dir, patterns):
    """
    Extracts only files matching specific regex patterns from a ZIP archive.
    Avoids extracting unneeded multi-gigabyte grids or PDF sections.
    Skips extraction if already extracted and file size matches.
    """
    extracted_paths = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.namelist():
            for pat in patterns:
                if re.search(pat, member, re.IGNORECASE):
                    target_path = os.path.join(extract_dir, member)
                    info = zf.getinfo(member)
                    if os.path.exists(target_path) and os.path.getsize(target_path) == info.file_size:
                        print(f"  [Using extracted] {member} ({info.file_size / (1024*1024):.2f} MB)")
                        out_path = target_path
                    else:
                        print(f"  Extracting {member} ({info.file_size / (1024*1024):.2f} MB)...")
                        out_path = zf.extract(member, path=extract_dir)
                    extracted_paths.append(out_path)
                    break
    return sorted(list(set(extracted_paths)))


# ==============================================================================
# AWS S3 STORAGE INTEGRATION
# ==============================================================================

class UploadProgressCallback:
    """
    Monitors byte stream during S3 upload and prints periodic progress percentages.
    """
    def __init__(self, filename, total_bytes):
        self.filename = filename
        self.total_bytes = total_bytes
        self.uploaded_bytes = 0
        self.start_time = time.time()
        self.last_print_time = time.time()
        self.last_pct = 0

    def __call__(self, bytes_amount):
        self.uploaded_bytes += bytes_amount
        now = time.time()
        pct = int((self.uploaded_bytes / self.total_bytes * 100)) if self.total_bytes > 0 else 100
        # Print progress update at >= 25% increments or every 3 seconds or on completion
        if (pct >= self.last_pct + 25) or (now - self.last_print_time >= 3.0) or (self.uploaded_bytes >= self.total_bytes):
            self.last_print_time = now
            self.last_pct = pct
            mb_uploaded = self.uploaded_bytes / (1024 * 1024)
            mb_total = self.total_bytes / (1024 * 1024)
            print(f"      -> {self.filename}: {mb_uploaded:.1f}/{mb_total:.1f} MB ({pct}%)", flush=True)


def get_s3_client():
    """
    Initializes and returns a boto3 S3 client using the standard AWS credential chain
    (environment variables, ~/.aws/credentials, or IAM roles). Never hard-codes keys.
    """
    region = os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION")
    client_kwargs = {
        "config": Config(
            retries={
                "max_attempts": 10,
                "mode": "adaptive"
            }
        )
    }
    if region:
        client_kwargs["region_name"] = region

    return boto3.client("s3", **client_kwargs)


def check_s3_object_exists(s3_client, bucket, key, min_size=10):
    """
    Verifies if an object exists in S3 with a valid size.
    """
    try:
        resp = s3_client.head_object(Bucket=bucket, Key=key)
        return resp.get("ContentLength", 0) >= min_size
    except Exception:
        return False


def check_survey_s3_status(survey_name, bucket, raw_prefix, s3_client=None):
    """
    Checks if all required files for a survey already exist in S3.
    """
    if not bucket:
        return False
    if s3_client is None:
        try:
            s3_client = get_s3_client()
        except Exception:
            return False

    prefix_clean = raw_prefix.strip("/") if raw_prefix else ""
    files = [
        f"{survey_name}.csv",
        f"{survey_name}.dfn.txt",
        f"{survey_name}.des.txt"
    ]
    for fname in files:
        key = f"{prefix_clean}/{survey_name}/{fname}" if prefix_clean else f"{survey_name}/{fname}"
        min_size = 10 * 1024 if fname.endswith(".csv") else 50
        if not check_s3_object_exists(s3_client, bucket, key, min_size=min_size):
            return False
    return True


def upload_file_to_s3(local_path, bucket, s3_key, s3_client=None):
    """
    Uploads a local file to S3 using boto3's upload_file with multipart configuration
    and AES256 server-side encryption.
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Local file not found for S3 upload: {local_path}")

    if s3_client is None:
        s3_client = get_s3_client()

    # Configure multipart upload for large datasets
    transfer_config = TransferConfig(
        multipart_threshold=64 * 1024 * 1024,
        multipart_chunksize=64 * 1024 * 1024,
        max_concurrency=4,
        use_threads=True
    )

    extra_args = {
        "ServerSideEncryption": "AES256"
    }

    file_size = os.path.getsize(local_path)
    file_name = os.path.basename(local_path)
    file_size_mb = file_size / (1024 * 1024)

    print(f"    [S3 Uploading] {file_name} ({file_size_mb:.2f} MB) -> s3://{bucket}/{s3_key}")
    sys.stdout.flush()

    progress = UploadProgressCallback(file_name, file_size)
    t0 = time.time()

    s3_client.upload_file(
        local_path,
        bucket,
        s3_key,
        ExtraArgs=extra_args,
        Config=transfer_config,
        Callback=progress
    )

    elapsed = time.time() - t0
    speed = file_size_mb / max(0.01, elapsed)
    print(f"    [S3 Upload] SUCCESS: s3://{bucket}/{s3_key} ({file_size_mb:.2f} MB in {elapsed:.1f}s, {speed:.1f} MB/s)")
    sys.stdout.flush()
    return True


def upload_survey_to_s3(survey_name, survey_dir, bucket, raw_prefix="processed_data/AusAEM_WA_EM_Data", s3_client=None):
    """
    Uploads all generated survey files (.csv, .dfn.txt, .des.txt) to AWS S3.
    Target structure:
      s3://<bucket>/<raw_prefix>/<survey_name>/<survey_name>.csv
      s3://<bucket>/<raw_prefix>/<survey_name>/<survey_name>.dfn.txt
      s3://<bucket>/<raw_prefix>/<survey_name>/<survey_name>.des.txt

    Preserves survey folder structure dynamically.
    Returns dict with upload status and uploaded keys.
    """
    if not bucket:
        print(f"  [S3] S3_BUCKET not configured. Skipping S3 upload for {survey_name}.")
        return {"status": "Skipped", "error": "S3_BUCKET not configured"}

    if s3_client is None:
        try:
            s3_client = get_s3_client()
        except Exception as e:
            print(f"  [S3 ERROR] Failed to initialize S3 client: {e}")
            return {"status": "Failed", "error": str(e)}

    files_to_upload = [
        f"{survey_name}.csv",
        f"{survey_name}.dfn.txt",
        f"{survey_name}.des.txt"
    ]

    prefix_clean = raw_prefix.strip("/") if raw_prefix else ""

    print(f"\n  [S3 UPLOAD] Uploading survey '{survey_name}' to s3://{bucket}/{prefix_clean}/{survey_name}/...")
    sys.stdout.flush()

    uploaded_keys = []
    failed_files = []

    for fname in files_to_upload:
        fpath = os.path.join(survey_dir, fname)
        if not os.path.exists(fpath):
            err_msg = f"Required output file not found: {fpath}"
            print(f"  [S3 ERROR] {err_msg}")
            failed_files.append((fname, err_msg))
            continue

        s3_key = f"{prefix_clean}/{survey_name}/{fname}" if prefix_clean else f"{survey_name}/{fname}"

        try:
            upload_file_to_s3(fpath, bucket, s3_key, s3_client=s3_client)
            uploaded_keys.append(s3_key)
        except (BotoCoreError, ClientError, Exception) as e:
            print(f"  [S3 ERROR] Failed to upload {fname} to s3://{bucket}/{s3_key}: {e}")
            failed_files.append((fname, str(e)))

    if failed_files:
        print(f"  [S3 ERROR] Survey '{survey_name}' upload completed with {len(failed_files)} failure(s).")
        return {
            "status": "Failed",
            "uploaded": uploaded_keys,
            "failures": failed_files
        }

    print(f"  [S3 COMPLETE] All {len(uploaded_keys)} files for '{survey_name}' successfully uploaded to S3.")
    return {
        "status": "Uploaded",
        "uploaded": uploaded_keys
    }


# ==============================================================================
# PIPELINE RUNNER FOR A SINGLE SURVEY
# ==============================================================================

def process_survey(survey_key, 
                   source_path=None, 
                   custom_url=None, 
                   output_root="AusAEM_WA_EM_Data", 
                   temp_dir="temp_work",
                   force_reprocess=False,
                   s3_bucket=None,
                   raw_prefix="processed_data/AusAEM_WA_EM_Data",
                   no_s3=False,
                   s3_client=None):
    """
    Executes the complete processing and upload lifecycle for an AusAEM survey:
    1. Checks if survey is already completed; preserves existing files if so.
    2. Obtains the package (local file or download).
    3. Locates airborne EM .dat, .dfn, and .des files.
    4. Converts .dfn into .dfn.txt and extracts columns.
    5. Converts .des into .des.txt.
    6. Streams and converts all .dat files into unified .csv.
    7. Validates CSV row counts and column structure.
    8. Cleans up temporary artifacts.
    9. Uploads completed files to AWS S3 if configured.
    """
    preset = OFFICIAL_SURVEYS.get(survey_key, {})
    survey_name = survey_key
    s3_enabled = bool(s3_bucket) and not no_s3
    
    print("\n" + "=" * 75)
    print(f"SURVEY AREA: {survey_name}")
    print(f"Official Name: {preset.get('name', survey_name)}")
    print(f"EM System: {preset.get('system', 'Airborne EM')}")
    print("=" * 75)
    
    survey_out_dir = os.path.join(output_root, survey_name)
    os.makedirs(survey_out_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)
    
    final_csv = os.path.join(survey_out_dir, f"{survey_name}.csv")
    final_dfn_txt = os.path.join(survey_out_dir, f"{survey_name}.dfn.txt")
    final_des_txt = os.path.join(survey_out_dir, f"{survey_name}.des.txt")
    
    # 0. Check if already completed locally
    if not force_reprocess and is_survey_completed(survey_out_dir, survey_name):
        print(f"  [ALREADY COMPLETED] Found verified existing files in: {survey_out_dir}")
        print(f"    - CSV:     {os.path.basename(final_csv)} ({os.path.getsize(final_csv)/(1024*1024):.2f} MB)")
        print(f"    - DFN.TXT: {os.path.basename(final_dfn_txt)}")
        print(f"    - DES.TXT: {os.path.basename(final_des_txt)}")
        
        # Count lines for summary table
        cols, _ = parse_dfn_file(final_dfn_txt)
        with open(final_csv, "r", encoding="utf-8", errors="ignore") as f:
            total_records = sum(1 for _ in f) - 1

        s3_res_status = "Skipped"
        if s3_enabled:
            # Check if already present in S3
            if check_survey_s3_status(survey_name, s3_bucket, raw_prefix, s3_client=s3_client):
                prefix_clean = raw_prefix.strip("/") if raw_prefix else ""
                print(f"  [S3] Verified all outputs already exist in S3: s3://{s3_bucket}/{prefix_clean}/{survey_name}/ (Preserved)")
                s3_res_status = "Preserved"
            else:
                print(f"  [S3] Survey outputs missing from S3. Uploading verified local outputs...")
                up_res = upload_survey_to_s3(survey_name, survey_out_dir, s3_bucket, raw_prefix, s3_client=s3_client)
                s3_res_status = up_res.get("status", "Failed")
        elif not s3_bucket and not no_s3:
            print("  [S3] S3_BUCKET not configured. Skipping S3 upload.")
            s3_res_status = "Skipped"
        else:
            s3_res_status = "Disabled"
            
        return {
            "survey_name": survey_name,
            "status": "Success (Preserved)",
            "s3_status": s3_res_status,
            "rows": total_records,
            "columns": len(cols),
            "csv_size_mb": round(os.path.getsize(final_csv) / (1024 * 1024), 2),
            "csv_path": os.path.abspath(final_csv),
            "dfn_path": os.path.abspath(final_dfn_txt),
            "des_path": os.path.abspath(final_des_txt)
        }
        
    # 1. Determine Source Package
    package_zip = None
    working_dir = None
    
    if source_path:
        if os.path.isdir(source_path):
            working_dir = source_path
        elif os.path.isfile(source_path) and source_path.lower().endswith(".zip"):
            package_zip = source_path
        else:
            raise ValueError(f"Invalid local source specified: {source_path}")
    else:
        url = custom_url or preset.get("url")
        if not url:
            raise ValueError(f"No source path or URL available for survey '{survey_name}'")
            
        zip_filename = os.path.basename(url.split("?")[0])
        package_zip = os.path.join(temp_dir, f"{survey_name}_{zip_filename}")
        
        if not os.path.exists(package_zip) or os.path.getsize(package_zip) < 1024:
            download_file_with_resume(url, package_zip)
        else:
            print(f"  Using existing cached package: {package_zip} ({os.path.getsize(package_zip)/(1024*1024):.2f} MB)")
            
    # 2. Extract / Locate Files
    extract_target = os.path.join(temp_dir, f"{survey_name}_extracted")
    os.makedirs(extract_target, exist_ok=True)
    
    if package_zip:
        print("  Scanning package for located airborne EM data (.dat, .dfn, .des)...")
        patterns = (preset.get("dat_patterns", [r"\.dat$"]) + 
                    preset.get("dfn_patterns", [r"\.dfn$"]) + 
                    preset.get("des_patterns", [r"\.des$"]))
        extract_matched_files_from_zip(package_zip, extract_target, patterns)
        working_dir = extract_target
        
    all_files = []
    for root, _, files in os.walk(working_dir):
        for f in files:
            all_files.append(os.path.join(root, f))
            
    # Match EM .dat files
    dat_patterns = preset.get("dat_patterns", [r".*EM.*\.dat$", r"\.dat$"])
    matched_dats = []
    for p in dat_patterns:
        for fpath in all_files:
            if re.search(p, fpath, re.IGNORECASE) and fpath not in matched_dats:
                if not re.search(r"grid|section|xyz|cnd", fpath, re.IGNORECASE):
                    matched_dats.append(fpath)
                    
    matched_dats = sorted(matched_dats)
    
    # Match .dfn
    dfn_patterns = preset.get("dfn_patterns", [r".*EM.*\.dfn$", r"\.dfn$"])
    matched_dfn = None
    for p in dfn_patterns:
        for fpath in all_files:
            if re.search(p, fpath, re.IGNORECASE):
                matched_dfn = fpath
                break
        if matched_dfn:
            break
            
    # Match .des
    des_patterns = preset.get("des_patterns", [r".*EM.*\.des$", r"\.des$"])
    matched_des = None
    for p in des_patterns:
        for fpath in all_files:
            if re.search(p, fpath, re.IGNORECASE):
                matched_des = fpath
                break
        if matched_des:
            break
            
    print(f"  Found {len(matched_dats)} EM data file(s):")
    for d in matched_dats:
        print(f"    - {os.path.basename(d)}")
    print(f"  Found DFN: {os.path.basename(matched_dfn) if matched_dfn else 'None'}")
    print(f"  Found DES: {os.path.basename(matched_des) if matched_des else 'None'}")
    
    if not matched_dats:
        raise FileNotFoundError(f"No matching airborne EM .dat files found for {survey_name}")
    if not matched_dfn:
        raise FileNotFoundError(f"No matching .dfn file found for {survey_name}")
    if not matched_des:
        raise FileNotFoundError(f"No matching .des file found for {survey_name}")
        
    # 3. Parse DFN & Create .dfn.txt
    print("  Parsing ASEG-GDF2 .dfn field definitions...")
    columns, field_meta = parse_dfn_file(matched_dfn)
    print(f"  Defined {len(field_meta)} fields -> {len(columns)} total CSV channels/columns")
    convert_dfn_to_readable_txt(matched_dfn, final_dfn_txt, columns)
    print(f"  Saved readable DFN: {final_dfn_txt}")
    
    # 4. Create .des.txt
    convert_des_to_readable_txt(matched_des, final_des_txt)
    print(f"  Saved readable DES: {final_des_txt}")
    
    # 5. Stream Convert .dat to CSV
    print("  Converting EM .dat dataset into CSV (streaming)...")
    total_records = convert_dat_files_to_csv(matched_dats, final_csv, columns)
    
    # 6. Validate Output
    print("  Validating generated CSV tabular file...")
    with open(final_csv, "r", encoding="utf-8") as f_chk:
        header_tokens = f_chk.readline().strip().split(",")
        sample_row = f_chk.readline().strip().split(",")
        
    if len(header_tokens) != len(columns):
        raise ValueError(f"Validation failure: header column count ({len(header_tokens)}) != expected ({len(columns)})")
    if len(sample_row) != len(columns):
        raise ValueError(f"Validation failure: first record token count ({len(sample_row)}) != expected ({len(columns)})")
        
    print(f"  Validation SUCCESSFUL: {total_records:,} rows x {len(columns)} columns correctly formed.")
    
    # 7. Cleanup Temporary Extraction Files
    if os.path.exists(extract_target):
        try:
            shutil.rmtree(extract_target)
            print("  Cleaned up temporary uncompressed files.")
        except Exception as e:
            print(f"  Warning cleaning extract dir: {e}")

    # 8. S3 Upload (Only after validation and metadata generation succeed)
    s3_res_status = "Skipped"
    if s3_enabled:
        up_res = upload_survey_to_s3(survey_name, survey_out_dir, s3_bucket, raw_prefix, s3_client=s3_client)
        s3_res_status = up_res.get("status", "Failed")
    elif not s3_bucket and not no_s3:
        print("  [S3] S3_BUCKET not configured. Skipping S3 upload.")
        s3_res_status = "Skipped"
    else:
        s3_res_status = "Disabled"
            
    return {
        "survey_name": survey_name,
        "status": "Success",
        "s3_status": s3_res_status,
        "rows": total_records,
        "columns": len(columns),
        "csv_size_mb": round(os.path.getsize(final_csv) / (1024 * 1024), 2),
        "csv_path": os.path.abspath(final_csv),
        "dfn_path": os.path.abspath(final_dfn_txt),
        "des_path": os.path.abspath(final_des_txt)
    }


# ==============================================================================
# MAIN ENTRYPOINT
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="AusAEM-WA Airborne Electromagnetic (AEM) Automated Downloader, Processor & AWS S3 Uploader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Usage Examples:
  python ausaem.py                         # Complete workflow: processes all 7 surveys and uploads to S3.
  python ausaem.py --survey Earaheedy      # Run a single survey area.
  python ausaem.py --no-s3                 # Run purely local processing without S3 upload.
  python ausaem.py --force                 # Force re-download and re-conversion even if already completed.
  python ausaem.py --output-dir MyData     # Custom local output directory.
  python ausaem.py --bucket my-s3-bucket   # Specify S3 bucket (or via S3_BUCKET env var).
  python ausaem.py --prefix custom/prefix  # Specify S3 key prefix (or via RAW_PREFIX / S3_PREFIX env var).
"""
    )
    parser.add_argument(
        "--survey", "-s",
        choices=list(OFFICIAL_SURVEYS.keys()),
        default=None,
        help="Optional: Run only a specific survey area (default: processes all 7 automatically)."
    )
    parser.add_argument(
        "--output-dir", "-o",
        default="AusAEM_WA_EM_Data",
        help="Root output directory (default: AusAEM_WA_EM_Data/)."
    )
    parser.add_argument(
        "--temp-dir",
        default=None,
        help="Optional: Temporary directory for downloads (default: auto-detected optimal drive)."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Optional: Force re-download and re-conversion even if survey is already completed."
    )
    parser.add_argument(
        "--no-s3",
        action="store_true",
        help="Optional: Disable S3 uploading for a local-only run."
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="Optional: AWS S3 bucket name (default: from S3_BUCKET environment variable)."
    )
    parser.add_argument(
        "--prefix",
        default=None,
        help="Optional: S3 key prefix (default: from RAW_PREFIX or S3_PREFIX environment variable)."
    )
    
    args = parser.parse_args()
    
    temp_dir = args.temp_dir or get_default_temp_dir()
    output_root = os.path.abspath(args.output_dir)
    os.makedirs(output_root, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)

    # Resolve S3 Configuration from CLI args and environment variables
    s3_bucket = (args.bucket or os.getenv("S3_BUCKET") or "").strip()
    raw_prefix = (args.prefix or os.getenv("RAW_PREFIX") or os.getenv("S3_PREFIX") or "processed_data/AusAEM_WA_EM_Data").strip()
    no_s3 = args.no_s3

    # Initialize S3 Client if S3 upload is enabled
    s3_client = None
    if not no_s3 and s3_bucket:
        try:
            s3_client = get_s3_client()
        except Exception as e:
            print(f"[WARNING] Could not initialize AWS S3 client: {e}")
            print("Local processing will proceed, but S3 uploads will be disabled.")
            s3_client = None
    
    # If no specific survey is passed, automatically run ALL 7 surveys
    surveys_to_run = [args.survey] if args.survey else list(OFFICIAL_SURVEYS.keys())
    
    print("==================================================================")
    print("     AusAEM-WA Automated Airborne Electromagnetic Processor       ")
    print("==================================================================")
    print(f" Execution Mode  : {'Single Survey (' + args.survey + ')' if args.survey else 'Full Automatic Statewide Pipeline (All 7 Surveys)'}")
    print(f" Timestamp       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" Output Root     : {output_root}")
    print(f" Temp Directory  : {temp_dir}")
    if no_s3:
        print(f" S3 Upload       : Disabled (--no-s3 flag specified)")
    elif s3_bucket:
        clean_pref = raw_prefix.strip('/')
        print(f" S3 Destination  : s3://{s3_bucket}/{clean_pref + '/' if clean_pref else ''}")
    else:
        print(f" S3 Upload       : Not configured (S3_BUCKET not set; running in local-only mode)")
    print(f" Total in Queue  : {len(surveys_to_run)} survey area(s)")
    print("==================================================================\n")
    
    summary_results = []
    t_start_all = time.time()
    
    for s_idx, s_key in enumerate(surveys_to_run, start=1):
        print(f"\n>>> [{s_idx}/{len(surveys_to_run)}] STARTING SURVEY: {s_key}")
        sys.stdout.flush()
        try:
            res = process_survey(
                survey_key=s_key,
                output_root=output_root,
                temp_dir=temp_dir,
                force_reprocess=args.force,
                s3_bucket=s3_bucket,
                raw_prefix=raw_prefix,
                no_s3=no_s3,
                s3_client=s3_client
            )
            summary_results.append(res)
        except Exception as e:
            print(f"\n[ERROR] Survey '{s_key}' encountered an issue: {e}")
            import traceback
            traceback.print_exc()
            summary_results.append({
                "survey_name": s_key,
                "status": f"Failed: {type(e).__name__}",
                "s3_status": "N/A",
                "rows": 0,
                "columns": 0,
                "csv_size_mb": 0.0,
                "csv_path": "N/A",
                "dfn_path": "N/A",
                "des_path": "N/A"
            })
            print(f"--> Continuing automatically to next survey in queue...")
            sys.stdout.flush()
            
    total_sec = time.time() - t_start_all
    
    # Final Comprehensive Summary Table
    print("\n" + "=" * 105)
    print("                               FINAL SURVEY PROCESSING SUMMARY                               ")
    print("=" * 105)
    print(f"{'Survey Area':<28} | {'Processing Status':<22} | {'S3 Upload':<12} | {'Records (Rows)':<14} | {'Cols':<5} | {'CSV Size (MB)':<12}")
    print("-" * 105)
    for res in summary_results:
        s3_stat = res.get("s3_status", "N/A")
        print(f"{res['survey_name']:<28} | {res['status']:<22} | {s3_stat:<12} | {res['rows']:>14,} | {res['columns']:>5} | {res['csv_size_mb']:>10.2f} MB")
    print("=" * 105)
    print(f"Total Workflow Execution Time: {total_sec:.1f} seconds ({total_sec/60:.2f} minutes)\n")
    print("Local processed survey files are saved in:")
    print(f"  {output_root}")
    if s3_bucket and not no_s3:
        clean_pref = raw_prefix.strip('/')
        print("AWS S3 destination path:")
        print(f"  s3://{s3_bucket}/{clean_pref + '/' if clean_pref else ''}\n")
    print()

if __name__ == "__main__":
    main()
