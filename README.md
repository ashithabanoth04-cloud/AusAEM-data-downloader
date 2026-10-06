# AusAEM WA Data Pipeline

This project downloads and processes official AusAEM airborne electromagnetic data for Western Australia and uploads the final datasets to AWS S3.

## Survey Areas

- Earaheedy
- Eastern Goldfields
- East Yilgarn–Albany Fraser
- Murchison
- Northern WA
- South West–Albany
- Western Resources Corridor

## Data Processing

The project:

1. Downloads the official AusAEM data packages.
2. Extracts the required EM data.
3. Uses the `.dfn` file to determine the correct field names and structure.
4. Converts the EM `.dat` data into CSV format.
5. Converts the `.dfn` and `.des` files into readable text files.
6. Uploads the final datasets to AWS S3.

## Final Output

The output contains:

- `.csv` — processed EM data
- `.dfn.txt` — field/channel definitions
- `.des.txt` — data description

## AWS Upload

The `s3_uploader.py` script uploads the generated `AusAEM_WA_EM_Data` folder to an AWS S3 bucket.

AWS credentials are not stored in the repository.

## Configuration

Create a local `.env` file using `.env.example` as a template.

Do not commit the real `.env` file or AWS credentials to GitHub.
