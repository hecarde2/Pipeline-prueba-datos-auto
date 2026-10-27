import pytest
from include import usgs_client as u


def test_payload_complete():
    data = {"type": "FeatureCollection", "metadata": {"count": 2}, "features": [{}, {}]}
    assert u.validate_payload(data)[0] == 2


@pytest.mark.parametrize("data", [{"type": "FeatureCollection", "metadata": {"count": 3}, "features": [{}, {}]},
                                  {"type": "FeatureCollection", "features": []}, {"type": "x"}, [], None])
def test_incomplete_payload_rejected(data):
    with pytest.raises(u.IncompleteDataError):
        u.validate_payload(data)
