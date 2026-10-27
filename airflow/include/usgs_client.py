"""Cliente USGS con garantía de completitud: si la API dice N eventos, llegan N o se falla."""
import hashlib
import logging
import os
from datetime import timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)

FEED_URL = os.getenv("USGS_FEED_URL", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson")
QUERY_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
COUNT_URL = "https://earthquake.usgs.gov/fdsnws/event/1/count"
MAX_ALLOWED = 20000          # límite duro del endpoint query
TIMEOUT = (5, 30)            # (conexión, lectura) segundos


class USGSUnavailable(Exception):
    """La API no respondió, devolvió error HTTP o JSON ilegible."""


class IncompleteDataError(Exception):
    """La API respondió, pero faltan eventos o el payload no es confiable."""


def _get(url, params=None):
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=Retry(
        total=3, backoff_factor=1.5, status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",), respect_retry_after_header=True)))
    s.headers["User-Agent"] = "SismoWatch/1.0"
    try:
        r = s.get(url, params=params, timeout=TIMEOUT)
    except requests.RequestException as e:
        raise USGSUnavailable(f"fallo de red: {type(e).__name__}") from e
    finally:
        s.close()
    if r.status_code != 200:
        raise USGSUnavailable(f"HTTP {r.status_code} en {url}")
    return r


def _json(r):
    try:
        return r.json()
    except ValueError as e:
        raise USGSUnavailable("respuesta no es JSON válido") from e


def validate_payload(data):
    """Devuelve (expected_count, features) o lanza IncompleteDataError."""
    if not isinstance(data, dict) or data.get("type") != "FeatureCollection":
        raise IncompleteDataError("payload no es una FeatureCollection")
    feats = data.get("features")
    if not isinstance(feats, list):
        raise IncompleteDataError("payload sin lista 'features'")
    expected = (data.get("metadata") or {}).get("count")
    if not isinstance(expected, int) or isinstance(expected, bool):
        raise IncompleteDataError("payload sin metadata.count")
    if len(feats) != expected:
        raise IncompleteDataError(f"la API declara {expected} eventos pero llegaron {len(feats)}")
    return expected, feats


def _item(source, ws, we, r, data):
    expected, feats = validate_payload(data)
    return {"source": source, "window_start": ws, "window_end": we, "expected": expected,
            "received": len(feats), "sha256": hashlib.sha256(r.content).hexdigest(), "payload": data}


def fetch_feed():
    r = _get(FEED_URL)
    return _item("feed_hour", None, None, r, _json(r))


def count_window(start, end):
    """Conteo independiente (endpoint /count) para cruzar contra lo descargado."""
    r = _get(COUNT_URL, {"format": "geojson", "starttime": start.isoformat(), "endtime": end.isoformat()})
    try:
        return int(_json(r)["count"])
    except (USGSUnavailable, KeyError, TypeError, ValueError):
        try:
            return int(r.text.strip())
        except ValueError as e:
            raise USGSUnavailable("respuesta de /count ilegible") from e


def fetch_window(start, end):
    """Descarga [start, end). Parte la ventana a la mitad si supera el máximo permitido."""
    n = count_window(start, end)
    if n > MAX_ALLOWED:
        if end - start <= timedelta(minutes=1):
            raise IncompleteDataError(f"ventana mínima con {n} eventos (> {MAX_ALLOWED})")
        mid = start + (end - start) / 2
        return fetch_window(start, mid) + fetch_window(mid, end)
    r = _get(QUERY_URL, {"format": "geojson", "starttime": start.isoformat(),
                         "endtime": end.isoformat(), "orderby": "time-asc"})
    item = _item("query_window", start, end, r, _json(r))
    if item["received"] != n:
        raise IncompleteDataError(f"/count dice {n} y /query entregó {item['received']} ({start}..{end})")
    return [item]
