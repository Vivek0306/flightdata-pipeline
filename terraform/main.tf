terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)
  project     = var.project_id
  region      = var.region
}

# GCS Bucket — raw data lake
resource "google_storage_bucket" "raw_bucket" {
  name          = "${var.project_id}-flight-raw"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 90 }  # auto-delete files older than 90 days (keeps it free)
  }
}

# BigQuery Dataset — staging (raw loaded data)
resource "google_bigquery_dataset" "staging" {
  dataset_id = "flight_staging"
  location   = var.region
}

# BigQuery Dataset — marts (dbt transformed data)
resource "google_bigquery_dataset" "marts" {
  dataset_id = "flight_marts"
  location   = var.region
}


resource "google_bigquery_table" "raw_flights" {
  dataset_id          = google_bigquery_dataset.staging.dataset_id
  table_id            = "raw_flights"
  deletion_protection = false

  schema = jsonencode([
    { name = "icao24",          type = "STRING" },
    { name = "callsign",        type = "STRING" },
    { name = "origin_country",  type = "STRING" },
    { name = "time_position",   type = "FLOAT"  },
    { name = "last_contact",    type = "FLOAT"  },
    { name = "longitude",       type = "FLOAT"  },
    { name = "latitude",        type = "FLOAT"  },
    { name = "baro_altitude",   type = "FLOAT"  },
    { name = "on_ground",       type = "BOOLEAN"},
    { name = "velocity",        type = "FLOAT"  },
    { name = "true_track",      type = "FLOAT"  },
    { name = "vertical_rate",   type = "FLOAT"  },
    { name = "geo_altitude",    type = "FLOAT"  },
    { name = "squawk",          type = "STRING" },
    { name = "ingested_at",     type = "TIMESTAMP" }
  ])
}