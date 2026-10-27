import pytest
from include.transform import feature_to_silver_row

GOOD = {"id": "us1", "properties": {"mag": 4.5, "time": 1700000000000, "updated": 1700000100000, "place": "10 km N of X, CA",
        "tsunami": 0, "sig": 312, "magType": "ml", "status": "reviewed", "type": "earthquake", "url": "http://u"},
        "geometry": {"type": "Point", "coordinates": [-122.1, 37.2, 8.5]}}


def test_good_feature():
    r = feature_to_silver_row(GOOD, 7)
    assert r["event_id"] == "us1" and r["magnitude"] == 4.5 and r["depth_km"] == 8.5 and r["bronze_id"] == 7


def test_null_magnitude_ok():
    f = {**GOOD, "properties": {**GOOD["properties"], "mag": None}}
    assert feature_to_silver_row(f, 1)["magnitude"] is None


@pytest.mark.parametrize("mut", [{"id": None}, {"geometry": None}, {"geometry": {"coordinates": [1]}},
                                 {"properties": {"time": "abc"}}, {"properties": {"time": 1, "mag": "x"}}])
def test_bad_feature_raises_valueerror(mut):
    with pytest.raises(ValueError):
        feature_to_silver_row({**GOOD, **mut}, 1)
