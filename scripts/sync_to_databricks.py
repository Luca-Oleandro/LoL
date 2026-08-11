# Synchronize permanent local archive to Databricks, clean up old patches from it

import json
import logging
import os
import time
from databricks.sdk import WorkspaceClient

import archive

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Total number of most recent patches to keep on Databricks (current included)
RETENTION_PATCH_COUNT = 2


def upload_with_retry(workspace_client, location, remote_path, max_retries=3):
    """Read a compressed file from the archive and upload it to Databricks,

    retrying with increasing backoff (5s, 10s, 15s...) if the upload fails.
    """
    data = archive.read_archived_json(location)
    payload = json.dumps(data).encode("utf-8")

    for attempt in range(1, max_retries + 1):
        try:
            workspace_client.files.upload(
                remote_path, contents=payload, overwrite=True
            )
            return
        except Exception as e:
            logger.warning(
                f"Upload failed for {remote_path} (attempt {attempt}/{max_retries}): {e}"
            )
            if attempt == max_retries:
                raise
            time.sleep(5 * attempt)


def sync_patches(conn, workspace_client, catalog, schema, volume, patches_to_keep):
    """Upload all not sync matches of the recent patches."""
    rows = archive.get_unsynced_matches(conn, patches_to_keep)
    logger.info(f"Found {len(rows)} matches to sync to Databricks")

    for match_id, patch, match_location, timeline_location in rows:
        match_remote = f"/Volumes/{catalog}/{schema}/{volume}/{patch}/matches/match_{match_id}.json"
        timeline_remote = f"/Volumes/{catalog}/{schema}/{volume}/{patch}/timelines/timeline_{match_id}.json"

        upload_with_retry(workspace_client, match_location, match_remote)
        upload_with_retry(workspace_client, timeline_location, timeline_remote)

        archive.mark_synced(conn, match_id)
        logger.info(f"Synced match {match_id} (patch {patch})")


def cleanup_old_patches(workspace_client, catalog, schema, volume, patches_to_keep):
    """Delete patch folders which are not in the retention window anymore."""
    if not patches_to_keep:
        logger.info("No patches in archive, skipping cleanup")
        return

    base_path = f"/Volumes/{catalog}/{schema}/{volume}"
    try:
        existing_dirs = [
            entry.name.rstrip("/")
            for entry in workspace_client.files.list_directory_contents(base_path)
            if entry.is_directory
        ]
    except Exception as e:
        logger.warning(f"Could not list {base_path}, skipping cleanup: {e}")
        return

    for patch_dir in existing_dirs:
        if patch_dir not in patches_to_keep:
            path_to_delete = f"{base_path}/{patch_dir}"
            logger.info(
                f"Deleting old patch folder from Databricks: {path_to_delete}"
            )
            try:
                workspace_client.files.delete_directory(
                    path_to_delete, recursive=True
                )
            except Exception as e:
                logger.error(
                    f"Failed to delete directory {path_to_delete}: {e}"
                )


def main():
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")

    if not host or not token:
        raise ValueError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")

    workspace_client = WorkspaceClient(host=host, token=token)

    catalog = "workspace"
    schema = "bronze"
    volume = "raw_data"

    conn = archive.get_connection()

    all_patches = archive.get_patches_present(conn)
    patches_to_keep = all_patches[:RETENTION_PATCH_COUNT]
    logger.info(f"Patches found locally/archive: {all_patches}")
    logger.info(f"Patches to keep on Databricks: {patches_to_keep}")

    sync_patches(conn, workspace_client, catalog, schema, volume, patches_to_keep)
    cleanup_old_patches(
        workspace_client, catalog, schema, volume, patches_to_keep
    )

    logger.info("Sync completed")
    conn.close()


if __name__ == "__main__":
    main()