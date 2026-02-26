import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
client     = bigquery.Client(project=PROJECT_ID)


def run_query(name: str, sql: str):
    print(f"Running: {name}...")
    job = client.query(sql)
    job.result()
    print(f"Done: {name}")


# ── fct_flights ──────────────────────────────────────────
# main fact table — one row per flight snapshot
# this is what dashboards and analysts will query most
FCT_FLIGHTS = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.flight_marts.fct_flights` AS

SELECT
    icao24,
    callsign,
    origin_country,
    latitude,
    longitude,
    altitude_feet,
    geo_altitude_feet,
    speed_kmh,
    vertical_rate_ftpm,
    heading_degrees,
    on_ground,
    squawk,
    flight_phase,
    flight_category,
    snapshot_time,
    ingested_at,

    -- derive if flight is international or domestic
    -- domestic = origin country is India
    CASE
        WHEN origin_country = 'India' THEN 'Domestic'
        ELSE 'International'
    END AS flight_type,

    -- bin speed into categories for easier analysis
    CASE
        WHEN speed_kmh IS NULL    THEN 'Unknown'
        WHEN speed_kmh < 300      THEN 'Slow (<300 km/h)'
        WHEN speed_kmh < 600      THEN 'Medium (300-600 km/h)'
        WHEN speed_kmh < 900      THEN 'Fast (600-900 km/h)'
        ELSE                           'Very Fast (>900 km/h)'
    END AS speed_category

FROM `{PROJECT_ID}.flight_staging.raw_flights`
"""


# ── dim_countries ────────────────────────────────────────
# aggregate flights by origin country
# answers: which countries have the most flights over India?
DIM_COUNTRIES = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.flight_marts.dim_countries` AS

SELECT
    origin_country,
    COUNT(DISTINCT icao24)              AS unique_aircraft,
    COUNT(*)                            AS total_flights,
    ROUND(AVG(speed_kmh), 2)            AS avg_speed_kmh,
    ROUND(AVG(altitude_feet), 2)        AS avg_altitude_feet,
    COUNTIF(on_ground = FALSE)          AS flights_airborne,
    COUNTIF(on_ground = TRUE)           AS flights_on_ground
FROM `{PROJECT_ID}.flight_staging.raw_flights`
WHERE origin_country IS NOT NULL
GROUP BY origin_country
ORDER BY total_flights DESC
"""


# ── dim_flight_phases ────────────────────────────────────
# breakdown of flights by phase
# answers: at any given snapshot, how many flights are cruising vs landing?
DIM_FLIGHT_PHASES = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.flight_marts.dim_flight_phases` AS

SELECT
    flight_phase,
    flight_category,
    COUNT(*)                            AS total_flights,
    ROUND(AVG(speed_kmh), 2)            AS avg_speed_kmh,
    ROUND(AVG(altitude_feet), 2)        AS avg_altitude_feet,
    ROUND(AVG(vertical_rate_ftpm), 2)   AS avg_vertical_rate_ftpm
FROM `{PROJECT_ID}.flight_staging.raw_flights`
WHERE flight_phase IS NOT NULL
GROUP BY flight_phase, flight_category
ORDER BY total_flights DESC
"""


# ── agg_flights_daily ────────────────────────────────────
# daily summary — accumulates over time as pipeline runs daily
# answers: how does flight traffic over India trend day over day?
AGG_DAILY = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.flight_marts.agg_flights_daily` AS

SELECT
    DATE(ingested_at)                   AS flight_date,
    COUNT(*)                            AS total_flights,
    COUNT(DISTINCT icao24)              AS unique_aircraft,
    COUNT(DISTINCT origin_country)      AS countries_represented,
    COUNTIF(on_ground = FALSE)          AS flights_airborne,
    COUNTIF(on_ground = TRUE)           AS flights_on_ground,
    COUNTIF(flight_type = 'Domestic')   AS domestic_flights,
    COUNTIF(flight_type = 'International') AS international_flights,
    ROUND(AVG(speed_kmh), 2)            AS avg_speed_kmh,
    ROUND(AVG(altitude_feet), 2)        AS avg_altitude_feet
FROM `{PROJECT_ID}.flight_marts.fct_flights`
WHERE ingested_at IS NOT NULL
GROUP BY flight_date
ORDER BY flight_date DESC
"""


# ── Main ─────────────────────────────────────────────────
def main():
    run_query("fct_flights",        FCT_FLIGHTS)
    run_query("dim_countries",      DIM_COUNTRIES)
    run_query("dim_flight_phases",  DIM_FLIGHT_PHASES)
    run_query("agg_flights_daily",  AGG_DAILY)
    print("\nAll transformations complete!")

if __name__ == "__main__":
    main()