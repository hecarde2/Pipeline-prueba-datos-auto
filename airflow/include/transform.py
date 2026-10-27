"""Bronze -> Silver: función pura y testeable. Aquí solo se convierten tipos; los rangos los valida quality.py."""
from .safe import epoch_ms_to_dt, to_float, to_int, to_text


def feature_to_silver_row(feature, bronze_id):
    if not isinstance(feature, dict):
        raise ValueError("feature no es un objeto")
    fid = feature.get("id")
    if not isinstance(fid, str) or not fid.strip():
        raise ValueError("feature sin id")
    props = feature.get("properties")
    geom = feature.get("geometry")
    if not isinstance(props, dict):
        raise ValueError(f"{fid}: properties inválido")
    coords = geom.get("coordinates") if isinstance(geom, dict) else None
    if not isinstance(coords, (list, tuple)) or len(coords) < 2:
        raise ValueError(f"{fid}: coordenadas inválidas")
    try:
        event_time = epoch_ms_to_dt(props.get("time"), "time")
        updated = props.get("updated")
        return {
            "event_id": fid.strip(),
            "event_time": event_time,
            "updated_at": epoch_ms_to_dt(updated, "updated") if updated is not None else event_time,
            "magnitude": to_float(props.get("mag"), "mag"),
            "mag_type": to_text(props.get("magType"), 20),
            "place": to_text(props.get("place"), 300),
            "longitude": to_float(coords[0], "longitude", required=True),
            "latitude": to_float(coords[1], "latitude", required=True),
            "depth_km": to_float(coords[2], "depth") if len(coords) > 2 else None,
            "tsunami": to_int(props.get("tsunami"), "tsunami"),
            "significance": to_int(props.get("sig"), "sig"),
            "status": to_text(props.get("status"), 20),
            "event_type": to_text(props.get("type"), 40),
            "url": to_text(props.get("url"), 500),
            "bronze_id": bronze_id,
        }
    except ValueError as e:
        raise ValueError(f"{fid}: {e}") from None
