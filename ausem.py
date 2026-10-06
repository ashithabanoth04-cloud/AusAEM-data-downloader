#!/usr/bin/env python3
"""
AusAEM-WA Airborne Electromagnetic (AEM) Automated Downloader & Processor
===========================================================================
A fully automated, zero-configuration Python tool to download, locate, parse,
and convert official AusAEM-WA airborne electromagnetic datasets into clean,
standardized tabular CSV and readable metadata files.

Default Usage (Runs everything automatically):
    python ausem.py

Features:
- Fully automated: Running `python ausem.py` with NO arguments automatically processes
  all seven AusAEM-WA survey areas from official GSWA / Geoscience Australia sources.
- Smart skip: Automatically detects already completed survey folders and verifies their
  integrity so completed datasets are preserved and not re-downloaded.
- Stream-based parser: Handles multi-gigabyte .dat files line-by-line without loading
  the entire dataset into memory.
- Dynamic schema extraction: Robustly interprets ASEG-GDF2 .dfn field definitions and
  expands multi-channel array columns (e.g. 15F12.6 -> Name[1]...Name[15]).
- Multi-block merger: Seamlessly combines multi-block surveys (e.g. Murchison Blocks E-H,
  South West Albany Blocks A-D) into a single unified CSV.
- Clean output structure: Keeps ONLY the final CSV, .dfn.txt, and .des.txt in the survey
  folders. All raw .dat files and temporary uncompressed files are removed.
- Fault-tolerant: If one survey encounters a network or parsing issue, it logs the error,
  records the status, and automatically continues to the remaining surveys.
- Detailed final summary table: Reports row counts, column counts, file sizes, and status
  for all seven survey areas.
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
# PIPELINE RUNNER FOR A SINGLE SURVEY
# ==============================================================================

def process_survey(survey_key, 
                   source_path=None, 
                   custom_url=None, 
                   output_root="AusAEM_WA_EM_Data", 
                   temp_dir="temp_work",
                   force_reprocess=False):
    """
    Executes the complete processing lifecycle for an AusAEM survey:
    1. Checks if survey is already completed; preserves existing files if so.
    2. Obtains the package (local file or download).
    3. Locates airborne EM .dat, .dfn, and .des files.
    4. Converts .dfn into .dfn.txt and extracts columns.
    5. Converts .des into .des.txt.
    6. Streams and converts all .dat files into unified .csv.
    7. Validates CSV row counts and column structure.
    8. Cleans up temporary artifacts.
    """
    preset = OFFICIAL_SURVEYS.get(survey_key, {})
    survey_name = survey_key
    
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
    
    # 0. Check if already completed
    if not force_reprocess and is_survey_completed(survey_out_dir, survey_name):
        print(f"  [ALREADY COMPLETED] Found verified existing files in: {survey_out_dir}")
        print(f"    - CSV:     {os.path.basename(final_csv)} ({os.path.getsize(final_csv)/(1024*1024):.2f} MB)")
        print(f"    - DFN.TXT: {os.path.basename(final_dfn_txt)}")
        print(f"    - DES.TXT: {os.path.basename(final_des_txt)}")
        
        # Count lines for summary table
        cols, _ = parse_dfn_file(final_dfn_txt)
        with open(final_csv, "r", encoding="utf-8", errors="ignore") as f:
            total_records = sum(1 for _ in f) - 1
            
        return {
            "survey_name": survey_name,
            "status": "Success (Preserved)",
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
            
    return {
        "survey_name": survey_name,
        "status": "Success",
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
        description="AusAEM-WA Airborne Electromagnetic (AEM) Automated Downloader & Processor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Usage:
  python ausem.py                  # Fully automatic: processes all 7 surveys with zero manual setup.
  python ausem.py --survey <Name>  # Optional: process only a single survey area.
  python ausem.py --force          # Optional: force re-download/re-process even if completed.
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
    
    args = parser.parse_args()
    
    temp_dir = args.temp_dir or get_default_temp_dir()
    output_root = os.path.abspath(args.output_dir)
    os.makedirs(output_root, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)
    
    # If no specific survey is passed, automatically run ALL 7 surveys
    surveys_to_run = [args.survey] if args.survey else list(OFFICIAL_SURVEYS.keys())
    
    print("==================================================================")
    print("     AusAEM-WA Automated Airborne Electromagnetic Processor       ")
    print("==================================================================")
    print(f" Execution Mode  : {'Single Survey (' + args.survey + ')' if args.survey else 'Full Automatic Statewide Pipeline (All 7 Surveys)'}")
    print(f" Timestamp       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" Output Root     : {output_root}")
    print(f" Temp Directory  : {temp_dir}")
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
                force_reprocess=args.force
            )
            summary_results.append(res)
        except Exception as e:
            print(f"\n[ERROR] Survey '{s_key}' encountered an issue: {e}")
            import traceback
            traceback.print_exc()
            summary_results.append({
                "survey_name": s_key,
                "status": f"Failed: {type(e).__name__}",
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
    print("\n" + "=" * 92)
    print("                               FINAL SURVEY PROCESSING SUMMARY                               ")
    print("=" * 92)
    print(f"{'Survey Area':<28} | {'Status':<20} | {'Records (Rows)':<14} | {'Cols':<5} | {'CSV Size (MB)':<12}")
    print("-" * 92)
    for res in summary_results:
        print(f"{res['survey_name']:<28} | {res['status']:<20} | {res['rows']:>14,} | {res['columns']:>5} | {res['csv_size_mb']:>10.2f} MB")
    print("=" * 92)
    print(f"Total Workflow Execution Time: {total_sec:.1f} seconds ({total_sec/60:.2f} minutes)\n")
    print("All processed survey files are saved in:")
    print(f"  {output_root}\n")

if __name__ == "__main__":
    main()
