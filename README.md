# AusAEM-WA Airborne Electromagnetic Data Processing Pipeline (AWS S3 & ECS Fargate)

A scalable Python tool and automated AWS ECS Fargate pipeline to download, process, and organize official **AusAEM-WA airborne electromagnetic (AEM) survey data** from Western Australian government and Geoscience Australia sources.

---

## 🌟 Key Features

- **Automated AusAEM-WA Data Download**: Downloads official airborne electromagnetic survey packages automatically from configured government data sources.

- **Dual Execution Modes**:
  - **AWS ECS Fargate Cloud Execution**: Runs the processing pipeline inside a Docker container on AWS ECS Fargate.
  - **Local Machine Execution**: Run the complete processing workflow directly from a local terminal using Python.

- **ASEG-GDF2 Data Processing**: Automatically identifies and processes `.dat`, `.dfn`, and `.des` files from AusAEM survey packages.

- **Streaming `.DAT` to CSV Conversion**: Processes large airborne EM datasets line-by-line without loading the complete dataset into memory.

- **Dynamic Schema Extraction**: Reads `.dfn` field definitions and expands multi-channel array fields into individual CSV columns.

- **Multi-Block Survey Support**: Combines multiple `.dat` blocks belonging to the same survey into a unified CSV dataset.

- **Fault-Tolerant Processing**: If one survey encounters an error, the pipeline records the failure and continues processing the remaining surveys.

- **Smart Skip**: Detects previously completed survey outputs and preserves them without unnecessary re-downloading or processing.

- **Clean Output Structure**: Stores the final CSV, readable DFN metadata, and DES metadata for each survey area.

---

## 📁 Supported Survey Areas

The pipeline is configured for the following AusAEM-WA survey areas:

```text
Earaheedy
Eastern_Goldfields
East_Yilgarn_Albany_Fraser
Murchison
South_West_Albany
Western_Resources_Corridor
Northern_WA
