# LoL Ranked Data Pipeline

An end-to-end data engineering pipeline that ingests League of Legends ranked match data from the Riot Games API and processes it through a Medallion Architecture (Bronze → Silver → Gold) on Databricks, producing analytics-ready tables — champion win rates by lane and patch, split by early vs. late game performance.

The pipeline is orchestrated end-to-end with Apache Airflow, deployed both locally (Docker) and on a cloud VM (Azure), and is built around two design constraints that shaped most of the architecture: **zero budget** and a **free-tier Databricks workspace with limited storage**.

## Architecture

```
Riot Games API
      │
      ▼
┌────────────────────┐
│ fetch_and_archive   │  Fetches apex-tier players (Challenger/Grandmaster/Master,
│  (ingestion.py)     │  EUW), their recent match IDs (rolling lookback window,
│                      │  not full history), and match+timeline JSON.
│                      │  Writes ONLY to the archive - never touches Databricks.
└────────────────────┘
      │
      ▼
┌────────────────────┐
│  Archive             │  Permanent, gzip-compressed JSON files organized by patch,
│  (archive.py)        │  with a SQLite index tracking what's archived and what's
│                       │  already synced to Databricks. Pluggable backend (local
│                       │  disk or Azure Blob Storage) behind a single env variable -
│                       │  ingestion.py never knows which one is active.
└────────────────────┘
      │
      ▼
┌────────────────────┐
│ sync_to_databricks   │  Independent of Riot entirely. Reads from the archive,
│                      │  uploads unsynced matches for the N most recent patches
│                      │  to the Databricks Volume (organized by patch), and
│                      │  deletes any older patch folders it finds.
└────────────────────┘
      │
      ▼
┌─────────────┐
│   Bronze     │  Incremental ingestion via Databricks Auto Loader (reads only
│              │  files never seen before - cost stays proportional to what's
│              │  new, not to total history). A retention step also prunes rows
│              │  outside the patch window directly from the Delta tables, since
│              │  Auto Loader append alone never deletes.
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
│   Gold       │  Aggregated analytics table: win rate by champion and lane for
│              │  the most recent patch (auto-derived, never hardcoded), split
│              │  by early vs. late game, with a minimum sample size filter.
└─────────────┘
```

**Why the raw archive and the Databricks working set are separate.** Databricks Free Edition has a limited storage quota, and a coach using the final tables only needs recent data, not months of history. So the pipeline keeps two tiers: a permanent, compressed archive (cheap, unbounded, lives outside Databricks) and a small "hot" working set on Databricks holding only the current patch plus a short retention window. Deduplication is decided entirely by the archive's SQLite catalog, never by what currently exists on Databricks - so pruning old data from Databricks never triggers a wasted re-fetch from Riot.

**Why ingestion and sync are two separate Airflow tasks.** They talk to two independent systems (Riot vs. Databricks) with independent failure modes. If a Databricks upload fails, the task retries without re-hitting Riot's rate limit for data that's already safely archived; if Riot has a transient outage, it doesn't block a Databricks sync of already-downloaded data.

## Tech Stack

- **Python** - ingestion, archiving, sync logic (`requests`, Databricks SDK, `sqlite3`, `gzip`, Azure Blob SDK)
- **Apache Airflow** - orchestration (Docker Compose locally, `DatabricksSubmitRunOperator` for remote notebook execution)
- **Docker** - containerized Airflow (custom image, `LocalExecutor`)
- **SQLite** - local catalog/index for the permanent raw archive (dedup + patch tracking)
- **Databricks** - compute (serverless), Unity Catalog, Delta Lake (Free Edition)
- **Delta Lake** - storage format; Auto Loader for incremental ingestion, `MERGE`/`DELETE` for Silver dedup and retention, `VACUUM` for physical cleanup
- **Spark SQL** - Bronze → Silver → Gold transformations
- **Riot Games API** - `league/v4` (apex-tier endpoints), `match-v5`
- **Azure** - VM deployment (cloud-hosted Airflow instance), Blob Storage (pluggable archive backend)

## Project Structure

```
LoL/
├── dags/
│   └── lol_pipeline_dag.py     # Airflow DAG: 5 sequential tasks, Riot to Gold
├── scripts/
│   ├── ingestion.py             # Riot API -> archive only
│   ├── archive.py               # Permanent archive: compressed JSON + SQLite index
│   │                             # (local disk or Azure Blob, pluggable)
│   ├── sync_to_databricks.py    # archive -> Databricks, with patch retention/cleanup
│   ├── bronze.py                # Bronze layer (Databricks notebook, source format)
│   ├── silver.py                # Silver layer (Databricks notebook, source format)
│   ├── gold.py                  # Gold layer (Databricks notebook, source format)
│   └── databricks_setup.py      # One-time catalog/schema setup (run manually once)
├── docker-compose.yaml          # Local Airflow (custom image, LocalExecutor)
├── Dockerfile                   # Airflow image with pinned dependencies
├── requirements.txt
├── .env.example
└── .gitignore
```

`bronze.py`, `silver.py`, and `gold.py` use the Databricks "notebook source" format (`# Databricks notebook source`, `# COMMAND ----------` cell markers) so they stay plain, diff-friendly Python files in Git while still importing directly into Databricks as fully interactive notebooks.

## Setup

1. Clone the repo:
   ```bash
   git clone https://github.com/Luca-Oleandro/LoL.git
   cd LoL
   ```
2. Generate and set Airflow's Fernet and secret keys (encrypts credentials and secures internal API calls between components):
   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   python -c "import secrets; print(secrets.token_hex(16))"
   ```
   Put both in a `.env` file (`AIRFLOW_FERNET_KEY=...`, `AIRFLOW_SECRET_KEY=...`) - never committed, already gitignored.
3. Build and start Airflow:
   ```bash
   docker compose build
   docker compose up -d
   ```
4. In the Airflow UI (`localhost:8080`, default `admin`/`admin` - local dev only, change before any non-local deployment), add these under **Admin → Variables**: `riot_api_key`, `databricks_host`, `databricks_token`.
5. On Databricks, run `scripts/databricks_setup.py` once to create the `bronze`/`silver`/`gold` schemas, then import `bronze.py`, `silver.py`, `gold.py` as notebooks and set their Workspace paths in the Airflow Connection / DAG (`databricks_default` connection, `notebook_path` in `lol_pipeline_dag.py`).
6. Trigger the `lol_ingestion` DAG.

## Usage

The DAG runs five tasks in sequence: `fetch_and_archive >> sync_to_databricks >> run_bronze >> run_silver >> run_gold`. To run pipeline scripts manually for debugging:
```bash
python scripts/ingestion.py           # Riot -> archive only
python scripts/sync_to_databricks.py  # archive -> Databricks, with retention cleanup
```

Key parameters, both top-of-file constants:
- `MATCH_LOOKBACK_DAYS` (`ingestion.py`) - how many days back to pull match history. Set low for daily runs (small safety margin over the actual gap since the last run); a much larger value is only needed for a one-off historical backfill.
- `RETENTION_PATCH_COUNT` (`sync_to_databricks.py`) - how many recent patches to keep on Databricks. The same window number is mirrored in the retention `DELETE` queries inside `bronze.py` and `silver.py` - if you change one, update all three.

Storage backend for the archive is controlled by `STORAGE_BACKEND` (`local` or `azure`), read by `archive.py`. Local is the default; switching to Azure Blob Storage requires only `AZURE_STORAGE_CONNECTION_STRING` and `AZURE_CONTAINER_NAME` as environment variables - no code changes.

## Design Decisions

A few choices worth explaining rather than leaving implicit:

- **Apex-tier only (Challenger/Grandmaster/Master), not the full ranked ladder.** These use dedicated, non-paginated Riot endpoints (a single call returns the whole league), keeping both API usage and data volume predictable. Lower tiers use a paginated endpoint returning far more players and would multiply both API calls and storage by orders of magnitude.
- **A rolling lookback window instead of full match history.** Riot's match-ID endpoint only returns each player's most recent games regardless of window size, so a short window (a couple of days, for daily runs) captures everything relevant without wasted calls on data almost certainly already archived.
- **A proactive, sliding-window rate limiter instead of reactive backoff.** The pipeline tracks its own request rate against the known Riot API limit and self-throttles before hitting a 429, rather than bursting and waiting out a fixed penalty after the fact. `Retry-After` is still honored as a safety net for anything the client-side limiter doesn't catch, and 5xx/network-level errors are retried separately from the (non-retryable) 4xx failures.
- **SQLite as the archive's catalog, not Databricks.** Whether a match has ever been downloaded is answered by the permanent archive, never by what currently exists on Databricks - otherwise, routine retention cleanup on Databricks would cause the same matches to be needlessly re-fetched from Riot.
- **Auto Loader over full re-reads for Bronze.** At the volume this pipeline reaches within a couple of patches, re-reading every historical file on each run stops being viable; Auto Loader's checkpoint makes each run's cost proportional to what's new, not to total history.

## Future Improvements

Roughly in priority order:

- **Coach-driven Gold layer metrics.** Currently reaching out to LoL coaches to validate which metrics are actually useful for scrim prep and VOD review (timeline-derived gold diffs at 10/15/20 min, objective timing, champion synergy in draft, matchup-specific win rates) before building them out.
- **Silver layer for timelines.** `b_timelines` is ingested into Bronze but not yet flattened into Silver; this unlocks the macro/timing metrics above and is the main differentiator versus win-rate sites like u.gg/op.gg.
- **Checkpointing in `get_matches_id`.** It currently builds the full match-ID set in memory before returning; a long interruption partway through loses that phase's progress entirely. Incremental persistence would make it resumable instead of restarting from zero.
- **Production Riot API key.** Currently on a Personal key (no 24h expiry, same rate limits as a dev key); a Production key would remove the remaining ceiling on ingestion volume and player pool size.
- **Unit tests** for the retention/dedup logic and the Bronze/Silver/Gold transformations.
- **Basic data visualization** on top of the Gold table.

## License

MIT - see [LICENSE](LICENSE).