from fastapi.testclient import TestClient
from app.main import app

c = TestClient(app)


def _login(u, p):
    return c.post("/auth/login", json={"username": u, "password": p})


def test_no_token_is_401():
    for path in ("/api/kpis", "/api/events", "/api/stream", "/api/reports", "/api/pipeline/status"):
        assert c.get(path).status_code == 401


def test_bad_login_is_401():
    assert _login("admin", "mala").status_code == 401


def test_viewer_cannot_do_admin_actions():
    tok = _login("viewer", "viewerpw").json()["access_token"]
    h = {"Authorization": f"Bearer {tok}"}
    assert c.post("/api/admin/backfill", json={"days": 5}, headers=h).status_code == 403
    assert c.post("/api/admin/reports/regenerate", json={"report_date": "2026-01-01"}, headers=h).status_code == 403
    assert c.get("/api/quality/failures", headers=h).status_code == 403


def test_input_validation():
    tok = _login("admin", "adminpw").json()["access_token"]
    r = c.post("/api/admin/backfill", json={"days": 9999}, headers={"Authorization": f"Bearer {tok}"})
    assert r.status_code == 422
