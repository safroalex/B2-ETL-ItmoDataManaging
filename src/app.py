import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict

import requests
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
LOGGER = logging.getLogger("weather-service")

YA_API_URL = "https://api.weather.yandex.ru/v2/forecast"


class GracefulShutdown:
    """Capture termination signals so the loop can exit cleanly."""

    def __init__(self) -> None:
        self._stop_requested = False
        signal.signal(signal.SIGINT, self._request_stop)
        signal.signal(signal.SIGTERM, self._request_stop)

    @property
    def stop_requested(self) -> bool:
        return self._stop_requested

    def _request_stop(self, *_: Any) -> None:
        LOGGER.info("Shutdown signal received, finishing current iteration...")
        self._stop_requested = True


def build_request(lat: str, lon: str, api_key: str) -> Dict[str, Any]:
    params = {
        "lat": lat,
        "lon": lon,
        "lang": "en_US",
        "limit": 1,
        "hours": False,
    }
    headers = {"X-Yandex-API-Key": api_key}
    return {"url": YA_API_URL, "headers": headers, "params": params}


def fetch_weather(lat: str, lon: str, api_key: str) -> Dict[str, Any]:
    request_spec = build_request(lat, lon, api_key)
    LOGGER.debug("Requesting weather: %s", json.dumps(request_spec["params"]))
    response = requests.get(
        request_spec["url"],
        headers=request_spec["headers"],
        params=request_spec["params"],
        timeout=15,
    )
    response.raise_for_status()
    payload: Dict[str, Any] = response.json()
    payload["fetched_at"] = datetime.now(timezone.utc).isoformat()
    return payload


def get_mongo_collection() -> Collection:
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_db = os.getenv("MONGO_DB", "weather")
    mongo_collection = os.getenv("MONGO_COLLECTION", "weather_raw")

    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    collection = db[mongo_collection]
    collection.create_index("fetched_at", background=True)
    return collection


def main() -> None:
    api_key = os.getenv("YANDEX_API_KEY")
    if not api_key:
        LOGGER.error("YANDEX_API_KEY is missing")
        sys.exit(1)

    lat = os.getenv("WEATHER_LAT", "55.75")
    lon = os.getenv("WEATHER_LON", "37.61")
    interval = int(os.getenv("POLLING_INTERVAL_SECONDS", "600"))

    LOGGER.info(
        "Starting weather ingestion loop (lat=%s, lon=%s, interval=%ss)",
        lat,
        lon,
        interval,
    )

    collection = get_mongo_collection()
    stopper = GracefulShutdown()

    while not stopper.stop_requested:
        try:
            payload = fetch_weather(lat, lon, api_key)
            result = collection.insert_one(payload)
            LOGGER.info(
                "Stored weather snapshot _id=%s, temp=%s",
                result.inserted_id,
                payload.get("fact", {}).get("temp"),
            )
        except requests.HTTPError as exc:
            LOGGER.error("Yandex Weather API error: status=%s body=%s", exc.response.status_code, exc.response.text)
        except (requests.RequestException, PyMongoError) as exc:
            LOGGER.exception("Transient error during ingestion: %s", exc)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.exception("Unexpected failure: %s", exc)

        for _ in range(interval):
            if stopper.stop_requested:
                break
            time.sleep(1)

    LOGGER.info("Weather ingestion service stopped")


if __name__ == "__main__":
    main()
