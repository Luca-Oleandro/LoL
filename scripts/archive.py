# Permanent local archive or Azure Cloud Storage for raw data
# Json files are compressed with gzip
# SQLite catalog tracks matches and their synchronization status to Databricks

import gzip
import json
import os
import sqlite3
from datetime import datetime, timezone

# Environment configuration: defines storage destination ('local' or 'azure') and directory paths
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "local")

ARCHIVE_DIR = os.getenv("ARCHIVE_DIR", "/opt/airflow/data/archive")
DB_PATH = os.getenv("ARCHIVE_DB_PATH", os.path.join(ARCHIVE_DIR, "archive.db"))

# Azure Blob Storage parameters
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER = os.getenv("AZURE_CONTAINER_NAME", "raw-data")

_azure_client = None


def _get_azure_client():
    """Lazy initialization for Azure Blob Storage client."""
    global _azure_client
    if _azure_client is None:
        from azure.storage.blob import BlobServiceClient

        if not AZURE_CONNECTION_STRING:
            raise ValueError(
                "AZURE_STORAGE_CONNECTION_STRING environment variable is not set."
            )
        _azure_client = BlobServiceClient.from_connection_string(
            AZURE_CONNECTION_STRING
        )
    return _azure_client


def get_connection():
    """Ensure archive directory exists, establish SQLite database connection, and initialize matches table."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT PRIMARY KEY,
            patch TEXT NOT NULL,
            downloaded_at TEXT NOT NULL,
            synced_at TEXT,
            match_location TEXT NOT NULL,
            timeline_location TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def is_archived(conn, match_id):
    """Check if a specific match_id is already present in the SQLite database catalog."""
    row = conn.execute(
        "SELECT 1 FROM matches WHERE match_id = ?", (match_id,)
    ).fetchone()
    return row is not None


def get_patch_from_match(match_data):
    """Extract game patch version from match metadata."""
    game_version = match_data["info"]["gameVersion"]
    parts = game_version.split(".")
    return f"{parts[0]}.{parts[1]}"


def _write_local(relative_path, data):
    """Save payload as a gzip-compressed JSON file on the local filesystem."""
    full_path = os.path.join(ARCHIVE_DIR, relative_path)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    with gzip.open(full_path, "wt", encoding="utf-8") as f:
        json.dump(data, f)
    return full_path


def _write_azure(relative_path, data):
    """Upload payload as a gzip-compressed JSON object to Azure Blob Storage."""
    from azure.storage.blob import ContentSettings

    blob_service = _get_azure_client()
    blob_client = blob_service.get_blob_client(
        container=AZURE_CONTAINER, blob=relative_path
    )
    compressed = gzip.compress(json.dumps(data).encode("utf-8"))
    blob_client.upload_blob(
        compressed,
        overwrite=True,
        content_settings=ContentSettings(
            content_encoding="gzip",
            content_type="application/json",
        ),
    )
    return f"https://{blob_service.account_name}.blob.core.windows.net/{AZURE_CONTAINER}/{relative_path}"


def archive_match(conn, match_id, match_data, timeline_data):
    """Save match and timeline files (locally or on Azure) and register entry into the SQLite catalog."""
    patch = get_patch_from_match(match_data)
    match_relpath = f"{patch}/matches/match_{match_id}.json.gz"
    timeline_relpath = f"{patch}/timelines/timeline_{match_id}.json.gz"

    if STORAGE_BACKEND == "azure":
        match_location = _write_azure(match_relpath, match_data)
        timeline_location = _write_azure(timeline_relpath, timeline_data)
    else:
        match_location = _write_local(match_relpath, match_data)
        timeline_location = _write_local(timeline_relpath, timeline_data)

    conn.execute(
        """INSERT INTO matches (match_id, patch, downloaded_at, synced_at, match_location, timeline_location)
           VALUES (?, ?, ?, NULL, ?, ?)""",
        (
            match_id,
            patch,
            datetime.now(timezone.utc).isoformat(),
            match_location,
            timeline_location,
        ),
    )
    conn.commit()


def read_archived_json(location):
    """Read a gzip-compressed JSON file from disk or Azure and parse it into a Python dictionary."""
    if location.startswith("http://") or location.startswith("https://"):
        blob_name = location.split(f"/{AZURE_CONTAINER}/")[-1]
        blob_service = _get_azure_client()
        blob_client = blob_service.get_blob_client(
            container=AZURE_CONTAINER, blob=blob_name
        )
        download_stream = blob_client.download_blob()
        decompressed_data = gzip.decompress(download_stream.readall())
        return json.loads(decompressed_data.decode("utf-8"))
    else:
        with gzip.open(location, "rt", encoding="utf-8") as f:
            return json.load(f)


def get_patches_present(conn):
    """Return all distinct patches present in the archive ordered from newest to oldest."""
    rows = conn.execute("SELECT DISTINCT patch FROM matches").fetchall()
    patches = [r[0] for r in rows]
    patches.sort(key=lambda p: tuple(int(x) for x in p.split(".")), reverse=True)
    return patches


def get_unsynced_matches(conn, patches):
    """Fetch all recorded matches for the given patches that have not been synced to Databricks yet."""
    if not patches:
        return []
    placeholders = ",".join("?" for _ in patches)
    rows = conn.execute(
        f"""SELECT match_id, patch, match_location, timeline_location
            FROM matches WHERE patch IN ({placeholders}) AND synced_at IS NULL""",
        patches,
    ).fetchall()
    return rows


def mark_synced(conn, match_id):
    """Update SQLite record to mark a match as successfully synced to Databricks."""
    conn.execute(
        "UPDATE matches SET synced_at = ? WHERE match_id = ?",
        (datetime.now(timezone.utc).isoformat(), match_id),
    )
    conn.commit()