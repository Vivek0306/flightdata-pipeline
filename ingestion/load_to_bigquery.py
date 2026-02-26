import os
import pytz
from datetime import datetime
from google.cloud import bigquery, storage
from dotenv import load_dotenv

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# ── Config ───────────────────────────────────────────────
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
PROJECT_ID  = os.getenv("GCP_PROJECT_ID")
DATASET     = "flight_staging"
TABLE       = "raw_flights"

IST         = pytz.timezone("Asia/Kolkata")
today       = datetime.now(IST).strftime("%Y-%m-%d")

# point to processed parquet folder instead of raw JSON
SOURCE_URI  = f"gs://{BUCKET_NAME}/processed/flights/{today}/*.parquet"
TABLE_REF   = f"{PROJECT_ID}.{DATASET}.{TABLE}"


# ── Load Parquet → BigQuery ──────────────────────────────
def load_to_bigquery():
    client = bigquery.Client(project=PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        source_format        = bigquery.SourceFormat.PARQUET,
        write_disposition    = bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect           = True,   # infer schema from parquet file
    )

    print(f"Loading from : {SOURCE_URI}")
    print(f"Loading into : {TABLE_REF}")

    load_job = client.load_table_from_uri(
        SOURCE_URI,
        TABLE_REF,
        job_config=job_config
    )

    # wait for the job to complete
    load_job.result()

    # confirm how many rows landed
    table = client.get_table(TABLE_REF)
    print(f"Successfully loaded {table.num_rows} rows into {TABLE_REF}")


# ── Main ─────────────────────────────────────────────────
def main():
    load_to_bigquery()
    print("Done!")

if __name__ == "__main__":
    main()