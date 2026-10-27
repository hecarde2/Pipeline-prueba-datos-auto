"""Reglas de calidad (validaciones propias en Python). Sin cuarentena: si algo falla, el lote se BLOQUEA."""
from collections import Counter
from datetime import datetime, timedelta, timezone

REQUIRED = ("event_id", "event_time", "latitude", "longitude")


def _check(name, passed, detail=""):
    return {"name": name, "passed": bool(passed), "detail": detail}


def run_checks(rows, expected_count, now=None):
    """Evalúa un lote ya tipado. Devuelve lista de checks; el lote pasa solo si TODOS pasan."""
    now = now or datetime.now(timezone.utc)
    out = [_check("completitud", len(rows) == expected_count,
                  f"esperados={expected_count} recibidos={len(rows)}")]
    missing = [r.get("event_id") for r in rows if any(r.get(k) is None for k in REQUIRED)]
    out.append(_check("campos_obligatorios", not missing, f"{len(missing)} filas incompletas {missing[:3]}"))
    dups = [k for k, c in Counter(r["event_id"] for r in rows).items() if c > 1]
    out.append(_check("id_unico", not dups, f"{len(dups)} ids duplicados {dups[:3]}"))
    bad_lat = [r["event_id"] for r in rows if r["latitude"] is not None and not -90 <= r["latitude"] <= 90]
    bad_lon = [r["event_id"] for r in rows if r["longitude"] is not None and not -180 <= r["longitude"] <= 180]
    out.append(_check("rango_latitud", not bad_lat, f"{len(bad_lat)} fuera de [-90,90] {bad_lat[:3]}"))
    out.append(_check("rango_longitud", not bad_lon, f"{len(bad_lon)} fuera de [-180,180] {bad_lon[:3]}"))
    bad_mag = [r["event_id"] for r in rows if r["magnitude"] is not None and not -2 <= r["magnitude"] <= 10.5]
    out.append(_check("rango_magnitud", not bad_mag, f"{len(bad_mag)} fuera de [-2,10.5] {bad_mag[:3]}"))
    bad_dep = [r["event_id"] for r in rows if r["depth_km"] is not None and not -10 <= r["depth_km"] <= 800]
    out.append(_check("rango_profundidad", not bad_dep, f"{len(bad_dep)} fuera de [-10,800] km {bad_dep[:3]}"))
    future = [r["event_id"] for r in rows if r["event_time"] and r["event_time"] > now + timedelta(minutes=5)]
    out.append(_check("fecha_no_futura", not future, f"{len(future)} eventos en el futuro {future[:3]}"))
    return out


def score(checks):
    total = len(checks)
    passed = sum(c["passed"] for c in checks)
    return {"evaluated": total, "passed": passed, "failed": total - passed,
            "score": round(100.0 * passed / total, 2) if total else 0.0,
            "status": "passed" if passed == total else "failed"}


def check_freshness(last_fetch, now=None, max_minutes=20):
    now = now or datetime.now(timezone.utc)
    if last_fetch is None:
        return _check("frescura", False, "no hay ninguna carga registrada")
    age = (now - last_fetch).total_seconds() / 60
    return _check("frescura", age <= max_minutes, f"última carga hace {age:.1f} min (máx {max_minutes})")


def check_volume(history, latest, min_samples=12):
    """Compara el último lote con el promedio de lotes anteriores del feed."""
    if len(history) < min_samples:
        return _check("volumen", True, f"histórico insuficiente ({len(history)}/{min_samples})")
    avg = sum(history) / len(history)
    ok = not (avg >= 20 and latest < avg * 0.1) and not (latest > avg * 10 + 50)
    return _check("volumen", ok, f"último={latest} promedio={avg:.1f}")
