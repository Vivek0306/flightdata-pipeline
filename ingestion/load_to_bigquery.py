import os
import pytz
from datetime import datetime
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# ── Config ───────────────────────────────────────────────
BUCKET_NAME  = os.getenv("GCS_BUCKET_NAME")
PROJECT_ID   = os.getenv("GCP_PROJECT_ID")
DATASET      = "flight_staging"
TABLE        = "raw_flights"
STAGING_TABLE = "raw_flights_staging"  # temporary table for dedup

IST          = pytz.timezone("Asia/Kolkata")
today        = datetime.now(IST).strftime("%Y-%m-%d")

SOURCE_URI   = f"gs://{BUCKET_NAME}/processed/flights/{today}/*.parquet"
TABLE_REF    = f"{PROJECT_ID}.{DATASET}.{TABLE}"
STAGING_REF  = f"{PROJECT_ID}.{DATASET}.{STAGING_TABLE}"

def table_exists(client, table_ref):
    """check if the target table exists"""
    try:
        client.get_table(table_ref)
        return True
    except Exception:
        return False

def load_to_bigquery():
    client = bigquery.Client(project=PROJECT_ID)

    # ── Step 1: load parquet into temp staging table ─────
    print(f"Loading Parquet into temp staging table: {STAGING_REF}")
    job_config = bigquery.LoadJobConfig(
        source_format     = bigquery.SourceFormat.PARQUET,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect        = True,
    )
    load_job = client.load_table_from_uri(
        SOURCE_URI,
        STAGING_REF,
        job_config=job_config
    )
    load_job.result()
    print("Temp staging table loaded successfully")

    # ── Step 2: first run vs subsequent runs ─────────────
    if not table_exists(client, TABLE_REF):
        # table doesn't exist yet — just rename staging to raw_flights
        print(f"raw_flights doesn't exist yet — creating from staging...")
        copy_job = client.copy_table(STAGING_REF, TABLE_REF)
        copy_job.result()
        print("Table created successfully!")
    else:
        # table exists — use MERGE to deduplicate
        print(f"Merging into {TABLE_REF} with deduplication...")
        merge_sql = f"""
        MERGE `{TABLE_REF}` AS target
        USING `{STAGING_REF}` AS source
        ON target.snapshot_time = source.snapshot_time
           AND target.icao24 = source.icao24

        WHEN MATCHED THEN
            UPDATE SET
                callsign            = source.callsign,
                origin_country      = source.origin_country,
                latitude            = source.latitude,
                longitude           = source.longitude,
                altitude_feet       = source.altitude_feet,
                geo_altitude_feet   = source.geo_altitude_feet,
                speed_kmh           = source.speed_kmh,
                vertical_rate_ftpm  = source.vertical_rate_ftpm,
                heading_degrees     = source.heading_degrees,
                on_ground           = source.on_ground,
                squawk              = source.squawk,
                flight_phase        = source.flight_phase,
                flight_category     = source.flight_category,
                ingested_at         = source.ingested_at

        WHEN NOT MATCHED THEN
            INSERT ROW
        """
        merge_job = client.query(merge_sql)
        merge_job.result()
        print("Merge complete!")

    # ── Step 3: drop temp staging table ──────────────────
    client.delete_table(STAGING_REF, not_found_ok=True)
    print("Temp staging table dropped")

    # confirm final row count
    table = client.get_table(TABLE_REF)
    print(f"Total rows in {TABLE_REF}: {table.num_rows}")


def main():
    load_to_bigquery()
    print("Done!")

if __name__ == "__main__":
    main()