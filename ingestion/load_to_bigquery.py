import os
import json
from datetime import datetime
from google.cloud import storage, bigquery
from dotenv import load_dotenv
import pytz

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET = "flight_staging"
TABLE = "raw_flights"
IST = pytz.timezone("Asia/Kolkata")

# ── Parse a single flight state ──────────────────────────
def parse_flight(state: list) -> dict:
    """
    OpenSky returns each flight as a plain list, not a dict.
    We manually map each index to a meaningful field name.
    Index reference: https://openskynetwork.github.io/opensky-api/rest.html
    """
    return {
        "icao24":           state[0],   # unique aircraft ID
        "callsign":         state[1].strip() if state[1] else None,
        "origin_country":   state[2],
        "time_position":    state[3],   # unix timestamp of last position update
        "last_contact":     state[4],   # unix timestamp of last contact with receiver
        "longitude":        state[5],
        "latitude":         state[6],
        "baro_altitude":    state[7],   # barometric altitude in meters
        "on_ground":        state[8],   # True if aircraft is on ground
        "velocity":         state[9],   # speed in m/s
        "true_track":       state[10],  # heading in degrees (0=North, 90=East)
        "vertical_rate":    state[11],  # climb/descent rate in m/s
        "geo_altitude":     state[13],  # GPS altitude in meters
        "squawk":           state[14],  # transponder code
        "ingested_at":      datetime.now(IST).isoformat(),  # when we loaded it
    }


# ── Read latest file from GCS ────────────────────────────
def get_latest_file():
    """
    Finds the most recently uploaded file in GCS.
    We sort all blobs by time_created and take the last one.
    """
    client = storage.Client()
    blobs = list(client.list_blobs(BUCKET_NAME, prefix="raw/flights/"))
    if not blobs:
        raise Exception("No files found in GCS bucket")

    latest = sorted(blobs, key=lambda b: b.time_created)[-1]
    print(f"Reading file: {latest.name}")

    content = latest.download_as_text()
    return json.loads(content)


# ── Load into BigQuery ───────────────────────────────────
def load_to_bigquery(rows: list):
    """
    Inserts rows into BigQuery using streaming inserts.
    This is the simplest way — no need to define a schema upfront,
    BigQuery will infer it. Good for getting started quickly.
    """
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    errors = client.insert_rows_json(table_ref, rows)

    if errors:
        print(f"Errors inserting rows: {errors}")
    else:
        print(f"Successfully inserted {len(rows)} rows into {table_ref}")


# ── Main ─────────────────────────────────────────────────
def main():
    raw = get_latest_file()
    states = raw.get("states", [])

    if not states:
        print("No flight states found in file")
        return

    print(f"Parsing {len(states)} flights...")
    rows = [parse_flight(s) for s in states]

    load_to_bigquery(rows)
    print("Done!")

if __name__ == "__main__":
    main()