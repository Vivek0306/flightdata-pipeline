import os
import json
import requests
from datetime import datetime, timezone
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# ── Config ──────────────────────────────────────────────
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Bounding box for India — only fetch flights over India
# format: min_lat, max_lat, min_lon, max_lon
INDIA_BBOX = (6.0, 37.0, 68.0, 97.0)


# ── Fetch from OpenSky ───────────────────────────────────
def fetch_flights():
    """
    Calls the OpenSky Network REST API and returns flight state vectors.
    We pass a bounding box so we only get flights currently over India.
    No API key needed for anonymous access (rate limited to 1 call per 10s).
    """
    min_lat, max_lat, min_lon, max_lon = INDIA_BBOX

    url = (
        f"https://opensky-network.org/api/states/all"
        f"?lamin={min_lat}&lamax={max_lat}&lomin={min_lon}&lomax={max_lon}"
    )
    
    print(f"Fetching flights from OpenSky...")  
    response = requests.get(url, timeout=30)
    response.raise_for_status()  # raises an error if request failed

    data = response.json()
    print(f"Fetched {len(data.get('states', []))} flights")
    return data


# ── Upload to GCS ────────────────────────────────────────
def upload_to_gcs(data: dict):
    """
    Uploads the raw JSON response to GCS.
    We partition by date so files are organized and easy to query later.
    e.g. raw/flights/2024-01-15/flights_143022.json
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # partition by date, unique filename by timestamp
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H-%M-%S")
    blob_path = f"raw/flights/{date_str}/flights_{time_str}.json"

    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        data=json.dumps(data),
        content_type="application/json"
    )

    print(f"Uploaded to gs://{BUCKET_NAME}/{blob_path}")
    return blob_path


# ── Main ─────────────────────────────────────────────────
def main():
    data = fetch_flights()
    if not data.get("states"):
        print("No flights found in bounding box, exiting.")
        return
    upload_to_gcs(data)
    print("Done!")

if __name__ == "__main__":
    main()