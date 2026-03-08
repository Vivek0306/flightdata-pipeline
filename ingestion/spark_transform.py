import os
import pytz
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, FloatType
from dotenv import load_dotenv

# ------------------------------ Configs ------------------------------
load_dotenv()

BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
CREDS_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

IST =  pytz.timezone('Asia/Kolkata')
today = datetime.now(IST).strftime('%Y-%m-%d')

INPUT_PATH = f'gs://{BUCKET_NAME}/raw/flights/{today}/'
OUTPUT_PATH = f'gs://{BUCKET_NAME}/processed/flights/{today}/'


SCHEMA = StructType((
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("time_position", FloatType(), True),
    StructField("last_contact", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("latitude", FloatType(), True),
    StructField("baro_altitude", FloatType(), True),
    StructField("on_ground", BooleanType(), True),
    StructField("velocity", FloatType(), True),
    StructField("true_track", FloatType(), True),
    StructField("vertical_rate", FloatType(), True),
    StructField("geo_altitude", FloatType(), True),
    StructField("squawk", StringType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("category", StringType(), True)
))


# ------------------------------ Spark Session ------------------------------
def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("FlightPipeline")
        .config(
            "spark.jars.packages",
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22"  # updated version
        )
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", CREDS_PATH)
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.driver.userClassPathFirst", "true")
        .config("spark.executor.userClassPathFirst", "true")
        .getOrCreate()
    )
    return spark

# ------------------------------ Read Raw Files ------------------------------
def read_raw_files(spark):
    try:
        df_json = spark.read.option("multiline", "true").json(INPUT_PATH)
        print(f"\nSuccessfully read from GCS!")
        df_json = df_json.dropDuplicates(["time"])
        print(f"Unique snapshots after dedup: {df_json.count()}")
        return df_json
    except Exception as e:
        print(f"\nUnable to read from GCS! Error occured: {e}")

# ------------------------------ Explode JSON Data ------------------------------
def explode_data(df):
    df_exploded = df.select(F.explode(F.col("states")).alias("flight"), F.col("time").alias("snapshot_time"))        
    return df_exploded

# ------------------------------ Map API Columns ------------------------------
def map_columns(df):
    df_mapped = df.select(
        F.col("snapshot_time"),
        F.col("flight").getItem(0).alias("icao24"),
        F.col("flight").getItem(1).alias("callsign"),
        F.col("flight").getItem(2).alias("origin_country"),
        F.col("flight").getItem(3).cast("double").alias("time_position"),
        F.col("flight").getItem(4).cast("double").alias("last_contact"),
        F.col("flight").getItem(5).cast("double").alias("longitude"),
        F.col("flight").getItem(6).cast("double").alias("latitude"),
        F.col("flight").getItem(7).cast("double").alias("baro_altitude"),
        F.col("flight").getItem(8).cast("boolean").alias("on_ground"),
        F.col("flight").getItem(9).cast("double").alias("velocity"),
        F.col("flight").getItem(10).cast("double").alias("true_track"),
        F.col("flight").getItem(11).cast("double").alias("vertical_rate"),
        F.col("flight").getItem(13).cast("double").alias("geo_altitude"),  
        F.col("flight").getItem(14).alias("squawk"),
        F.col("flight").getItem(16).cast("integer").alias("category")
    )
    return df_mapped

# ------------------------------ Filter Data ------------------------------
def filter_data(df):
    df_filtered = df.filter(F.col('icao24').isNotNull() & F.col('callsign').isNotNull() 
                                & F.col('latitude').isNotNull() & F.col('longitude').isNotNull())
    return df_filtered

# ------------------------------ Data Transformatios ------------------------------
def transform_data(df):
    df_transformed = df.withColumn('callsign', F.upper(F.trim(F.col('callsign'))))\
    .withColumn('altitude_feet', F.round(F.col('baro_altitude') * 3.2804, 2))\
    .withColumn('geo_altitude_feet', F.round(F.col('geo_altitude') * 3.2804, 2))\
    .withColumn('speed_kmh', F.round(F.col('velocity') * 3.6, 2))\
    .withColumn('vertical_rate_ftpm', F.round(F.col('vertical_rate') * 196.85, 2))\
    .withColumnRenamed("true_track", "heading_degrees")\
    .withColumn('ingested_at', F.to_timestamp(F.current_timestamp(), 'Asia/Kolkata'))\
    .withColumn("flight_category",
                F.when(F.col("category") == 0,  "No Info")
                .when(F.col("category") == 1,  "No ADS-B Emitter")
                .when(F.col("category") == 2,  "Light Aircraft")
                .when(F.col("category") == 3,  "Small Aircraft")
                .when(F.col("category") == 4,  "Large Aircraft")
                .when(F.col("category") == 5,  "High Vortex Large Aircraft")
                .when(F.col("category") == 6,  "Heavy Aircraft")
                .when(F.col("category") == 7,  "High Performance Aircraft")
                .when(F.col("category") == 8,  "Rotorcraft")
                .when(F.col("category") == 9,  "Glider")
                .when(F.col("category") == 10, "lighter Than Air")
                .when(F.col("category") == 11, "Parachutist")
                .when(F.col("category") == 12, "Ultralight")
                .when(F.col("category") == 15, "UAV")
                .when(F.col("category") == 16, "Surface Vehicle - Emergency")
                .when(F.col("category") == 17, "Surface Vehicle - Service")
                .otherwise("Unknown")
            )\
    .withColumn("flight_phase",
        F.when(F.col("on_ground") == True,                     "On Ground")
         .when(F.col("baro_altitude") < 1000,                  "Takeoff / Landing")
         .when(F.col("baro_altitude").between(1000, 6000),     "Climbing / Descending")
         .otherwise(                                            "Cruising")
    )\
    .drop('baro_altitude', 'geo_altitude', 'velocity', 'vertical_rate', "category")
    
    return df_transformed

# ------------------------------ Data Quality Check ------------------------------
def quality_check(df):
    print("\n=== Data Quality Report ===")
    print(f"Total rows: {df.count()}")

    print("\nNull counts per column:")
    df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in df.columns
    ]).show(truncate=False)

    print("Flight phase distribution:")
    df.groupBy("flight_phase")\
        .count()\
        .orderBy(F.col("count").desc())\
        .show()

    print("Top 10 origin countries:")
    df.groupBy("origin_country")\
        .count()\
        .orderBy(F.col("count").desc())\
        .limit(10)\
        .show()

# ------------------------------ Write Data to JSON ------------------------------
def write_data(df):
    print(f"\nWriting transformed data to: {OUTPUT_PATH}")

    df.write.mode("overwrite").parquet(OUTPUT_PATH)

    print(f"Successfully written to: {OUTPUT_PATH}")
    print(f"Total rows written: {df.count()}")


def main():
    spark = create_spark_session()
    try:
        df_raw = read_raw_files(spark)
        df_exploded = explode_data(df_raw)
        df_mapped = map_columns(df_exploded)
        df_filtered = filter_data(df_mapped)
        df_transformed = transform_data(df_filtered)
        quality_check(df_transformed)
        write_data(df_transformed)
        print("\nPipeline complete!")
    except Exception as e:
        print(f"Data Processing Failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
