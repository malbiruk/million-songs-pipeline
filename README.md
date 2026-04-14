# Million Songs: Genres, Lyrics & Trends

## Problem Statement

**What defines a genre musically and lyrically, and how has popular music changed over time?**

Genres are often defined by cultural context, but do they have measurable audio and lyrical signatures? This project builds a batch data pipeline that joins three complementary datasets from the Million Song Dataset ecosystem into a unified analytical warehouse to explore:

- How audio characteristics of popular music have shifted over the decades
- Which audio features distinguish genres from each other
- What words are most characteristic of each genre
- Whether some genres are lyrically richer than others
- How genre prevalence varies geographically

## Datasets

- [Million Song Dataset](http://millionsongdataset.com/) (1M tracks) — audio features and metadata (tempo, loudness, key, year, etc.) via the MSD summary file (300 MB HDF5)
- [MusixMatch](http://millionsongdataset.com/musixmatch) — bag-of-words lyrics for 237k tracks (top 5000 stemmed words), SQLite
- [tagtraum genre annotations](http://www.tagtraum.com/msd_genre_datasets.html) (CD2C) — genre labels for 191k tracks across 15 genres, text

All datasets are joined on `track_id`.

## Architecture

```mermaid
graph TD
    MSD[MSD summary - HDF5]
    MXM[MusixMatch - SQLite]
    TAG[tagtraum - text]

    subgraph Cloud
        GCS[(GCS data lake — raw/)]
        GCS2[(GCS data lake — processed/)]
        load[flows/load_bq.py]
        BQ[(BigQuery DWH)]
        dbt[flows/run_dbt.py]
        subgraph CR[Cloud Run Jobs]
            ingest[jobs/ingest.py]
            transform[jobs/transform.py]
        end
    end

    subgraph Local
        Prefect
        ST[Streamlit dashboard]
    end

    MSD --> ingest
    MXM --> ingest
    TAG --> ingest
    ingest --> GCS
    GCS --> transform
    transform --> GCS2
    GCS2 --> load
    load --> BQ
    BQ --> dbt
    dbt --> BQ
    BQ --> ST

    Prefect -.orchestrates.-> ingest
    Prefect -.orchestrates.-> transform
    Prefect -.orchestrates.-> load
    Prefect -.orchestrates.-> dbt
```

Prefect orchestrates locally while compute runs on GCP: ingest and transform as Cloud Run Jobs, load_bq and dbt as API calls to BigQuery. A multi-stage Dockerfile produces two images — a `jobs` image with worker dependencies for Cloud Run, and a `local` image with orchestrator and dashboard dependencies for docker compose.

## Technologies

- **Cloud**: Google Cloud Platform (GCS, BigQuery, Cloud Run Jobs, Artifact Registry, Cloud Build)
- **Infrastructure as Code**: Terraform
- **Workflow orchestration**: Prefect
- **Data warehouse**: BigQuery (tracks partitioned by year, tables clustered by genre/artist/word)
- **Batch processing**: Python (h5py + pyarrow for HDF5/SQLite → Parquet)
- **Transformations**: dbt
- **Dashboard**: Streamlit + Plotly

## Dashboard

<p float="left">
  <img src="images/dashboard-1.png" width="49%" />
  <img src="images/dashboard-2.png" width="49%" />
</p>

5 interactive panels:

1. **Audio features over time** — normalized tempo, loudness, duration, hotttnesss, % major key with 95% CI bands; filterable by genre
2. **Genre audio fingerprints** — radar chart comparing genres on normalized audio features
3. **Top words by genre** — most frequent words per genre (stop words filtered, adjustable word length)
4. **Lyrical diversity by genre** — vocabulary richness, unique words, total words per song
5. **Genre map** — choropleth: dominant genre per country or genre prevalence as % of artists

*styled with [tidepool-theme](https://github.com/malbiruk/tidepool-theme)*

## How to Reproduce

### Prerequisites

- [gcloud CLI](https://cloud.google.com/sdk/docs/install) (authenticated with `gcloud auth login`)
- GCP project with billing enabled
- Docker Compose
- [Terraform](https://www.terraform.io/)

### Setup and Run

```bash
git clone https://github.com/malbiruk/million-songs-pipeline.git
cd million-songs-pipeline
./setup.sh         # creates GCP service account, .env, provisions infrastructure
docker compose up  # starts Prefect, runs the pipeline, starts the dashboard
```

- http://localhost:4200 — Prefect UI (monitor pipeline execution)
- http://localhost:8501 — Dashboard (available after pipeline completes)

End-to-end (`setup.sh` through to a populated dashboard) takes roughly 10 minutes on a fresh GCP project.

### Teardown

```bash
./cleanup.sh
```

Stops the local containers, destroys all terraform-managed GCP resources, deletes the pipeline service account, and removes local state (`.keys/`, terraform state files).
