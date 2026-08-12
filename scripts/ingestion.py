# Fetch and archive new matches and timelines from Riot API into archive
from datetime import datetime, timedelta, timezone
import logging
import os
import time
import requests
import archive

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Number of days to look back for match history
MATCH_LOOKBACK_DAYS = 2

request_times = []


def safe_request(url, headers):
    """Request response, proactively respect rate limit and avoid 429 status code."""
    while True:
        # Keep track of requests in the last 120 seconds (sliding window)
        now = time.monotonic()
        request_times[:] = [t for t in request_times if now - t < 120]

        if len(request_times) >= 95:
            wait_time = 120 - (now - request_times[0])
            if wait_time > 0:
                time.sleep(wait_time)

        request_times.append(time.monotonic())

        try:
            response = requests.get(url, headers=headers, timeout=30)
        except requests.exceptions.RequestException as e:
            logger.warning(
                f"network error ({e.__class__.__name__}), retrying in 10s"
            )
            time.sleep(10)
            continue

        if response.status_code == 200:
            return response
        elif response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            logger.warning(
                f"rate limit exceeded, waiting {retry_after}s (server-specified)"
            )
            time.sleep(retry_after)
        elif 500 <= response.status_code < 600:
            logger.warning(
                f"server error {response.status_code}, retrying in 10s"
            )
            time.sleep(10)
        else:
            logger.error(f"error, status ={response.status_code}")
            response.raise_for_status()


def get_apex_players(queue, headers):
    """Fetch all Challenger, Grandmaster, Master players."""
    tiers = {
        "CHALLENGER": "challengerleagues",
        "GRANDMASTER": "grandmasterleagues",
        "MASTER": "masterleagues",
    }
    all_entries = []
    for tier_name, endpoint in tiers.items():
        url = f"https://euw1.api.riotgames.com/lol/league/v4/{endpoint}/by-queue/{queue}"
        data = safe_request(url, headers).json()
        entries = data.get("entries", [])
        logger.info(f"Found {len(entries)} players in {tier_name}")
        all_entries.extend(entries)
    logger.info(
        f"Found {len(all_entries)} total apex players (Challenger+Grandmaster+Master)"
    )
    return all_entries


def get_matches_id(queue_id, puuids, headers, lookback_days):
    """Return all match IDs played by the given list of PUUIDs within chosen lookback_days."""
    since = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    start_time = int(since.timestamp())

    matches = set()
    for puuid in puuids:
        url = (
            f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
            # Get first 100 matches in the last lookback_days for the given queue
            f"?queue={queue_id}&type=ranked&startTime={start_time}&start=0&count=100"
        )
        request = safe_request(url, headers)
        matches.update(request.json())
    logger.info(
        f"Found {len(matches)} unique matches (last {lookback_days} days)"
    )
    return matches


def fetch_and_archive_matches(matches, headers, conn):
    """Download each new match and timeline and save it in archive."""
    for match in matches:
        url_match = (
            f"https://europe.api.riotgames.com/lol/match/v5/matches/{match}"
        )
        url_timeline = f"https://europe.api.riotgames.com/lol/match/v5/matches/{match}/timeline"
        match_data = safe_request(url_match, headers).json()
        timeline_data = safe_request(url_timeline, headers).json()
        archive.archive_match(conn, match, match_data, timeline_data)
        logger.info(f"Archived match {match}")


def main():
    api_key = os.getenv("RIOT_API_KEY")
    headers = {"X-Riot-Token": api_key}

    queue = "RANKED_SOLO_5x5"
    queue_id = 420  # Solo/Duo Ranked queue ID
    conn = archive.get_connection()
    try:
        players = get_apex_players(queue, headers)
        puuids = [player["puuid"] for player in players if "puuid" in player]

        matches = get_matches_id(
            queue_id, puuids, headers, lookback_days=MATCH_LOOKBACK_DAYS
        )

        new_matches = {m for m in matches if not archive.is_archived(conn, m)}
        logger.info(f"Found {len(new_matches)} new matches to process")

        fetch_and_archive_matches(new_matches, headers, conn)

        logger.info("Ingestion completed")
    finally:
        conn.close()

if __name__ == "__main__":
    main()