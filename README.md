# Spotify Streaming ETL Pipeline

A custom-built data pipeline that extracts personal Spotify streaming history, enriches it via Spotify's API, transforms the data, and loads it into a star-schema PostgreSQL data warehouse. This project supports both music and podcast analytics.

# Tech Stack
- Python 3.12
- PostgreSQL
- Spotipy (Spotify Web API client)
- Psycopg2
- SQL (DDL, DML, time zone conversions, date normalization)
- Pytest

# Project Structure

```
├── scripts/
│   ├── connectors/
│   │   └── db_manager.py
│   │   └── spotify_client.py
│   ├── etl/
│   │   ├── extractor.py
│   │   ├── transformer.py
│   │   └── etl.py
│   └── main.py               # Runs the pipeline
├── data/
│   └── raw/                  # Local Spotify export .json files
├── config/
│   ├── config.py             # Load all the env variables
│   └── logging_config.py
├── tests/                    # Pytest directory
├── logs/                    
├── docs/
│   ├── images/               
│   └── sql/                  # All DDLs + date dim generation scripts
├── README.md
├── .gitignore
├── .env
└── requirements.txt
```

# Features
- Extracts raw streaming data from Spotify's personal export files and inserts it into staging
- Identifies new unique tracks, episodes, artists, and podcasts
- Fetches metadata from the Spotify API in batches (handles rate limits and errors)
- Stores raw data in a staging schema (inside jsonb columns)
- Transforms and loads clean, normalized records into a star schema
- Populates fact tables with calculated fields (e.g. session length category)
- Maintains re-runnable logic with deduplication and delta loads
- Tracks failed API responses for manual review
- Truncates staging layer after the process is done
- Every function and exception is logged to a local rotating log file

# Database Schemas
## Staging Layer
![Staging Schema](docs/images/staging.png)

Staging contains 5 tables, one for the raw data extracted from jsons, and 4 tables for the raw data fetched fromt the API with the following structure:
```
Table staging.spotify_*_data {
  "record_id" serial 
  "spotify_*_uri" varchar
  "raw_data" jsonb
  "fetched_at" timestamp [default: `CURRENT_TIMESTAMP`]
  "is_processed" boolean [default: false]
}
```

## Core Layer
![Core Schema](docs/images/core.png)

Core layer is designed as a star schema, with the following tables:
### Fact tables:
- `fact_tracks_history`: stores facts about streaming music
- `fact_podcasts_history`: stores facts about streaming podcasts
### Shared dimensions:
- `dim_date`: calendar from 2018 to 2030 
- `dim_time`: time dimension
- `dim_reason`: stores reasons for starting/ending a streaming session
### Exclusive dimensions for `fact_tracks_history`:
- `dim_artist`: stores data about each artists
- `dim_track`: stores data about each track
### Exclusive dimensions for `fact_podcasts_history`:
- `dim_podcast`: stores data about each podcast
- `dim_episode`: stores data about each episode

## Etl Internal Layer
![Core Schema](docs/images/etl_internal.png)

This layer has two tables:
- `failed_uris`: stores data about spotify URIs that returned nulls from the API
- `parent_tracks`: this table acts as a lookup table to populate parent_track_id column inside the dim_track (not yet implemented). So that during analysis, for example, a live/delux/extended track could be easily assosiated with the original track.

# Challenges faced
- Ensuring code reusability across multiple item types (tracks, artists, podcasts, and episodes).
- Batch processing while handling API rate limits and errors.
- Some Spotify API responses contain null values instead of valid track/episode data. So I had to find a way to detect, log, and retry failed URIs, and if it still fails, log it into the failed_uris table.
- Some raw tracks data has invalid or missing data inside, like a realease date set to '0000' or missing cover art URL.