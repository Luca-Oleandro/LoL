# LoL Ranked Data Pipeline

End-to-end data engineering pipeline that ingests League of Legends ranked match data from the Riot Games API and processes it through a Medallion Architecture (Bronze → Silver → Gold) on Databricks, producing analytics-ready tables for coaches — champion win rates, macro decision-making metrics, and matchup data by patch.

> **Status**: actively developed. This README reflects the current architecture, which has evolved significantly since the initial version — see [Changelog](#changelog--from-the-original-version) below if you're comparing against an earlier commit.

## Overview

The pipeline pulls match and timeline data for apex-tier players (Challenger, Grandmaster, Master) in EUW ranked solo queue, and separates two concerns that used to be coupled in a single script:

- A **permanent local archive** (compressed, SQLite-indexed) holding every match ever ingested — the raw dataset for future feature engineering and ML work.
- A **Databricks working set** holding only the current patch and a short retention window, kept lean deliberately to fit Databricks Free Edition's storage limits and to match what a coach actually needs (recent meta, not full history).

The final Gold layer answers concrete analytical questions for coaches — win rates by role and patch, early-vs-late-game power curves, and (planned) timeline-derived metrics like gold differentials and objective timing, based directly on coach feedback (see [Future Improvements](#future-improvements)).

The pipeline is idempotent and orchestrated end-to-end with Apache Airflow, running two decoupled DAG tasks so a failure in one never forces a redundant re-run of the other.

## Architecture

```
Riot Games API
      │
      ▼
┌──────────────────┐
│ fetch_and_archive │  Python + Airflow task. Fetches apex-tier players (Challenger/
│  (ingestion.py)   │  Grandmaster/Master), their recent match IDs (rolling lookback
│                   │  window, not full history), and match+timeline JSON.
│                   │  Writes ONLY to the local archive — never touches Databricks.
└──────────────────┘
      │
      ▼
┌──────────────────┐
│  Local Archive     │  Permanent, compressed (gzip) JSON files organized by patch,
│  (local_archive.py)│  with a SQLite index tracking what's archived and what's
│                     │  already synced to Databricks. Lives outside Databricks
│                     │  entirely — this is the durable dataset for future ML work.
└──────────────────┘
      │
      ▼
┌──────────────────┐
│ sync_to_databricks │  Second Airflow task, independent of Riot entirely. Reads
│                    │  from the local archive, uploads unsynced matches for the
│                    │  N most recent patches to the Databricks Volume (organized
│                    │  by patch), and deletes any older patch folders it finds.
└──────────────────┘
      │
      ▼
┌─────────────┐
│   Bronze     │  Raw JSON loaded into Delta tables (b_matches, b_timelines) via
│              │  MERGE — deduplicated. A retention step also prunes rows outside
│              │  the patch window directly from the Delta tables (MERGE alone
│              │  never deletes, so this closes that gap explicitly).
└─────────────┘
      │
      ▼
┌─────────────┐
│   Silver     │  Cleaned, flattened, typed tables: general_info (one row per
│              │  match), player_info (one row per player per match, via
│              │  EXPLODE). Remakes filtered out (games < 14 min excluded).
└─────────────┘
      │
      ▼
┌─────────────┐
│   Gold       │  Aggregated analytics tables — win rate by champion/role/patch,
│              │  matchup-level stats, and (planned) timeline-derived macro
│              │  metrics, shaped directly by feedback from coaches using the
│              │  product.
└─────────────┘
```

**Orchestration**: two Apache Airflow tasks, `fetch_and_archive >> sync_to_databricks`, run daily. Splitting ingestion (talks to Riot) from sync (talks to Databricks) means a Databricks-side failure never triggers a redundant, rate-limit-costly re-fetch from Riot, and vice versa.

**Storage backend is pluggable**: `local_archive.py` writes to local disk by default but is structured so raw storage can be swapped to S3 with a single environment variable (`STORAGE_BACKEND=s3`) and no changes to `ingestion.py` — useful if/when the archive moves off a personal machine.

## Tech Stack

- **Python** — ingestion, archiving, sync logic (`requests`, Databricks SDK, `sqlite3`, `gzip`)
- **Apache Airflow** — orchestration (Docker Compose, custom image with pinned dependencies)
- **Docker** — local Airflow deployment (custom `Dockerfile`, `LocalExecutor`)
- **SQLite** — local catalog/index for the permanent raw archive (dedup + patch tracking)
- **Databricks** — compute, Unity Catalog, Delta Lake (Free Edition)
- **Delta Lake** — storage format, `MERGE` for deduplication, explicit retention `DELETE`s
- **Spark SQL** — Bronze → Silver → Gold transformations
- **Riot Games API** — `league/v4` (apex-tier endpoints), `match-v5`

## Setup

1. Clone the repo
   ```bash
   git clone https://github.com/Luca-Oleandro/LoL.git
   cd LoL
   ```
2. Copy `.env.example` to `.env` for local script testing (production credentials are managed as Airflow Variables, not `.env` — see below).
3. Build and start Airflow:
   ```bash
   docker compose build
   docker compose up -d
   ```
4. In the Airflow UI (`localhost:8080`), add the following under **Admin → Variables**: `riot_api_key`, `databricks_host`, `databricks_token`.
5. Trigger the `lol_ingestion_v1` DAG.

## Usage

The DAG runs both pipeline stages in sequence. To run components manually for debugging:
```bash
python scripts/ingestion.py           # Riot -> local archive only
python scripts/sync_to_databricks.py  # local archive -> Databricks, with retention cleanup
```

Then run the Bronze, Silver, and Gold notebooks in order on a Databricks cluster to refresh each analytical layer.

The pipeline is parameterizable: `MATCH_LOOKBACK_DAYS` and `RETENTION_PATCH_COUNT` are top-of-file constants in `ingestion.py` / `sync_to_databricks.py`, and the apex tiers pulled (Challenger/Grandmaster/Master) are configurable in `get_apex_players`.

## Future Improvements

Roughly in priority order:

- **Coach-driven Gold layer metrics** — reaching out to coaches to validate which metrics are actually useful for scrim prep / VOD review (timeline-derived gold diffs at 10/15/20 min, objective timing, matchup-specific win rates) before building them out, rather than guessing.
- **Silver layer for timelines** — `b_timelines` is currently ingested into Bronze but not yet flattened into Silver; this unlocks the macro/timing metrics above and is the main differentiator versus existing sites like u.gg/op.gg.
- **Cloud deployment of Airflow** — moving the orchestrator off a personal machine onto a free-tier cloud VM, decoupling pipeline uptime from the developer's PC. The local raw archive stays local by design (cost); only the orchestration/coach-facing layer moves to the cloud.
- **Gold layer `MERGE`** — currently uses `CREATE OR REPLACE TABLE` (full refresh) per patch; incremental `MERGE` would avoid reprocessing history on each run.
- **Unit tests** for transformation logic and the retention/dedup logic in `local_archive.py`.
- **Production Riot API key** — currently on a Personal key (no 24h expiry, same rate limits as dev key); would remove the remaining scale ceiling on ingestion volume.
- **Checkpointing in `get_matches_id`** — currently builds the full match-ID set in memory before returning; a long-running interruption loses that phase's progress. Incremental persistence would make it resumable.

## Project Structure

```
LoL/
├── dags/
│   └── lol_pipeline_dag.py     # Airflow DAG: fetch_and_archive >> sync_to_databricks
├── scripts/
│   ├── ingestion.py             # Riot API -> local archive only
│   ├── local_archive.py         # Permanent archive: compressed JSON + SQLite index
│   └── sync_to_databricks.py    # local archive -> Databricks, with patch retention/cleanup
├── Bronze_layer.ipynb           # Raw JSON -> Delta tables (MERGE + retention DELETE)
├── Silver_layer.ipynb           # Cleaned, flattened, remake-filtered tables
├── Gold_layer.ipynb             # Aggregated analytics tables
├── Setup.ipynb                  # Catalog/schema creation
├── docker-compose.yaml          # Local Airflow (custom image, LocalExecutor)
├── Dockerfile                   # Airflow image with pinned dependencies
├── requirements.txt
├── .env.example
└── .gitignore
```

## Changelog — from the original version

The project originally downloaded straight into Databricks with no retention strategy, no orchestration, and no local archive. Since then:

- Ingestion and Databricks sync were split into two independent, separately-retryable steps.
- Added a permanent, compressed local archive with a SQLite catalog, decoupling "have I ever downloaded this match" from "is it still on Databricks" — needed once patch-based retention meant Databricks would no longer hold everything.
- Added patch-based retention on both the Databricks Volume and the Bronze Delta tables (the latter needed separately, since `MERGE` alone never deletes).
- Replaced the paginated `league-exp` endpoint (205 players/page) with the dedicated apex-tier endpoints, and expanded from Challenger-only to Challenger + Grandmaster + Master.
- Added Apache Airflow orchestration (Docker, custom image, credentials via Airflow Variables instead of `.env`).
- Rate limiting rewritten from a reactive fixed backoff to a proactive sliding-window limiter honoring `Retry-After`; added retry handling for transient 5xx errors and for Databricks upload failures.