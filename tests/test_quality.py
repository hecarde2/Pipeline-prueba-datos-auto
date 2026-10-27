from datetime import datetime, timezone
from include import quality


def row(i="a", lat=10.0, lon=20.0, mag=3.0, depth=5.0):
    return {"event_id": i, "event_time": datetime.now(timezone.utc), "latitude": lat, "longitude": lon,
            "magnitude": mag, "depth_km": depth}


def test_valid_batch_passes():
    c = quality.run_checks([row("a"), row("b")], 2)
    assert quality.score(c)["status"] == "passed"


def test_invalid_batch_fails():
    c = quality.run_checks([row("a", lat=95), row("a")], 3)       # lat inválida, id duplicado, conteo incompleto
    failed = {x["name"] for x in c if not x["passed"]}
    assert {"rango_latitud", "id_unico", "completitud"} <= failed
    assert quality.score(c)["status"] == "failed"


def test_volume_and_freshness():
    assert not quality.check_volume([100] * 20, 5)["passed"]
    assert quality.check_volume([100] * 20, 95)["passed"]
    assert not quality.check_freshness(None)["passed"]
