import json
import logging
import os
from datetime import datetime, timezone
import random
from typing import Any, Dict, Optional

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
LOGGER = logging.getLogger("weather-service")

YA_API_URL = "https://api.weather.yandex.ru/v2/forecast"


def get_mongo_collection() -> Collection:
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_db = os.getenv("MONGO_DB", "weather")
    mongo_collection = os.getenv("MONGO_COLLECTION", "weather_raw")

    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    collection = db[mongo_collection]
    collection.create_index("fetched_at", background=True)
    return collection


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


class IngestRequest(BaseModel):
    lat: Optional[str] = Field(default=None, description="Latitude override")
    lon: Optional[str] = Field(default=None, description="Longitude override")
    fetched_at: Optional[str] = Field(
        default=None,
        description="Optional fetched_at override (ISO-8601, UTC recommended) - useful for demos/backfills.",
    )


class IngestResponse(BaseModel):
    mongo_id: str
    fetched_at: str
    temperature_c: Optional[float] = None
    source: str = "yandex"


app = FastAPI(
    title="Weather Ingestion Service",
    version="1.0.0",
    description="HTTP service that fetches weather data from Yandex Weather API and stores raw payloads in MongoDB.",
)


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def generate_mock_weather(lat: str, lon: str, fetched_at: str | None = None) -> Dict[str, Any]:
    temp = round(random.uniform(-10, 30), 1)
    humidity = int(random.uniform(20, 95))
    pressure_mm = int(random.uniform(720, 780))
    pressure_pa = pressure_mm * 133
    return {
        "lat": float(lat),
        "lon": float(lon),
        "fact": {
            "temp": temp,
            "humidity": humidity,
            "pressure_mm": pressure_mm,
            "pressure_pa": pressure_pa,
        },
        "fetched_at": fetched_at or datetime.now(timezone.utc).isoformat(),
        "_source": "mock",
    }


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/ingest", response_model=IngestResponse)
def ingest(body: IngestRequest | None = None) -> IngestResponse:
    body = body or IngestRequest()
    lat = body.lat or os.getenv("WEATHER_LAT", "55.75")
    lon = body.lon or os.getenv("WEATHER_LON", "37.61")
    fetched_at_override = body.fetched_at

    if fetched_at_override:
        try:
            # Validate isoformat early (accepts both Z and +00:00 with normalization handled by fromisoformat)
            datetime.fromisoformat(fetched_at_override.replace("Z", "+00:00"))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid fetched_at (expected ISO-8601)") from exc

    fallback_to_mock = _truthy(os.getenv("WEATHER_FALLBACK_TO_MOCK", "false"))
    api_key = os.getenv("YANDEX_API_KEY")

    try:
        if not api_key:
            if fallback_to_mock:
                payload = generate_mock_weather(lat, lon, fetched_at_override)
            else:
                raise HTTPException(status_code=500, detail="YANDEX_API_KEY is missing")
        else:
            payload = fetch_weather(lat, lon, api_key)
            if fetched_at_override:
                payload["fetched_at"] = fetched_at_override
        collection = get_mongo_collection()
        result = collection.insert_one(payload)
        return IngestResponse(
            mongo_id=str(result.inserted_id),
            fetched_at=str(payload.get("fetched_at")),
            temperature_c=(payload.get("fact", {}) or {}).get("temp"),
            source=str(payload.get("_source") or "yandex"),
        )
    except requests.HTTPError as exc:
        if fallback_to_mock:
            payload = generate_mock_weather(lat, lon, fetched_at_override)
        else:
            raise HTTPException(
                status_code=502,
                detail=f"Yandex Weather API error: status={exc.response.status_code}",
            ) from exc
    except (requests.RequestException, PyMongoError) as exc:
        LOGGER.exception("Ingestion failed: %s", exc)
        if fallback_to_mock:
            payload = generate_mock_weather(lat, lon, fetched_at_override)
        else:
            raise HTTPException(status_code=500, detail="Ingestion failed") from exc

    # Fallback path: store synthetic payload
    try:
        collection = get_mongo_collection()
        result = collection.insert_one(payload)
        return IngestResponse(
            mongo_id=str(result.inserted_id),
            fetched_at=str(payload.get("fetched_at")),
            temperature_c=(payload.get("fact", {}) or {}).get("temp"),
            source=str(payload.get("_source") or "mock"),
        )
    except PyMongoError as exc:
        LOGGER.exception("Mongo write failed during fallback: %s", exc)
        raise HTTPException(status_code=500, detail="Mongo write failed") from exc