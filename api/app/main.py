import asyncio
import csv
import io
import json
import logging
import os
import re
import time
from datetime import date, datetime, timedelta, timezone

import boto3
import requests
from botocore.client import Config
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, Response
from pydantic import BaseModel, Field

from . import agent, config
from .db import DBUnavailable, fetch_all
from .security import authenticate, create_token, current_user, require_admin

log = logging.getLogger("api")
app = FastAPI(title="SismoWatch API (MinIO)", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=config.CORS_ORIGINS, allow_credentials=False,
                   allow_methods=["GET", "POST"], allow_headers=["Authorization", "Content-Type"])


@app.exception_handler(DBUnavailable)
async def _db_down(_, exc):
    return JSONResponse({"detail": "Almacenamiento no disponible."}, status_code=503)


@app.exception_handler(Exception)
async def _unexpected(_, exc):
    log.exception("error no controlado")
    return JSONResponse({"detail": "Error interno."}, status_code=500)


class LoginIn(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=128)


@app.post("/auth/login")
def login(body: LoginIn):
    role = authenticate(body.username, body.password)
    if not role:
        raise HTTPException(401, "Usuario o contraseña incorrectos")
    return {"access_token": create_token(body.username, role), "token_type": "bearer", "role": role}


@app.get("/health")
def health():
    return {"status": "ok", "storage": "minio"}


@app.get("/api/kpis")
def kpis(user=Depends(current_user)):
    try:
        from datetime import datetime, timedelta, timezone
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        k = fetch_all("SELECT count(*) AS events_24h, max(CAST(magnitude AS DOUBLE)) AS max_magnitude, round(avg(CAST(magnitude AS DOUBLE)), 2) AS avg_magnitude, 0 AS tsunami_alerts, max(loaded_at) AS last_update FROM events WHERE event_time >= ?", (cutoff,))[0]
        if k.get("events_24h") is None:
            k["events_24h"] = 0
        q = fetch_all("SELECT score, status, run_ts FROM quality_runs ORDER BY run_ts DESC LIMIT 1")
        k["quality_score"] = float(q[0]["score"]) if q else 100.0
        return k
    except Exception:
        try:
            k = fetch_all("SELECT count(*) AS events_24h, max(CAST(magnitude AS DOUBLE)) AS max_magnitude, round(avg(CAST(magnitude AS DOUBLE)), 2) AS avg_magnitude, 0 AS tsunami_alerts, max(loaded_at) AS last_update FROM events")[0]
            q = fetch_all("SELECT score, status, run_ts FROM quality_runs ORDER BY run_ts DESC LIMIT 1")
            k["quality_score"] = float(q[0]["score"]) if q else 100.0
            return k
        except Exception as e:
            raise


@app.get("/api/events")
def events(start: datetime | None = None, end: datetime | None = None,
           min_mag: float | None = Query(None, ge=-2, le=10), region: str | None = Query(None, max_length=80),
           q: str | None = Query(None, max_length=80), sort: str = "loaded_at",
           order: str = Query("desc", pattern="^(asc|desc)$"), limit: int = Query(50, ge=1, le=500),
           offset: int = Query(0, ge=0), user=Depends(current_user)):
    if sort not in {"event_time", "magnitude", "depth_km", "region", "place", "loaded_at"}:
        raise HTTPException(422, "Campo de orden no permitido")
    where, p = ["TRUE"], []
    if start: where.append("event_time >= ?"); p.append(start)
    if end: where.append("event_time <= ?"); p.append(end)
    if min_mag is not None: where.append("CAST(magnitude AS DOUBLE) >= ?"); p.append(min_mag)
    if region: where.append("region = ?"); p.append(region)
    if q: where.append("place ILIKE ?"); p.append(f"%{q}%")
    order_sql = f"ORDER BY {sort} {order} NULLS LAST, event_time DESC, event_id"
    rows = fetch_all(f"""SELECT event_id, event_time, magnitude, mag_type, place, region, longitude, latitude,
                                depth_km, tsunami, url, loaded_at, count(*) OVER() AS total
                         FROM events WHERE {' AND '.join(where)}
                         {order_sql} LIMIT ? OFFSET ?""", tuple(p + [limit, offset]))
    total = rows[0]["total"] if rows else 0
    for r in rows:
        r.pop("total", None)
    return {"total": total, "limit": limit, "offset": offset, "items": rows}


@app.get("/api/stats/timeseries")
def timeseries(hours: int = Query(48, ge=1, le=720), user=Depends(current_user)):
    try:
        return fetch_all("SELECT hour, events, max_magnitude, avg_magnitude FROM hourly_stats ORDER BY hour")
    except Exception:
        return []


@app.get("/api/stats/summary")
def summary(hours: int = Query(24, ge=1, le=720), user=Depends(current_user)):
    try:
        return {
            "by_magnitude": fetch_all("SELECT mag_bucket, count(*) AS events FROM events GROUP BY 1 ORDER BY 1"),
            "top_regions": fetch_all("SELECT region, count(*) AS events FROM events GROUP BY 1 ORDER BY 2 DESC LIMIT 10")}
    except Exception:
        return {"by_magnitude": [], "top_regions": []}


@app.get("/api/reports")
def reports(q: str | None = Query(None, max_length=80), limit: int = Query(20, ge=1, le=100),
            offset: int = Query(0, ge=0), user=Depends(current_user)):
    try:
        return fetch_all("SELECT report_date, headline, summary, highlights, generated_by, model, prompt_version, created_at FROM daily_reports ORDER BY report_date DESC LIMIT ? OFFSET ?", (limit, offset))
    except Exception:
        return []


@app.get("/api/reports/export")
def export_reports(user=Depends(current_user)):
    rows = fetch_all("SELECT report_date, headline, summary, generated_by, model FROM daily_reports ORDER BY report_date DESC")
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["fecha", "titular", "resumen", "generado_por", "modelo"])
    for r in rows:
        w.writerow([str(c) if c is None or str(c)[:1] not in "=+-@" else "'" + str(c) for c in r.values()])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=reportes_sismowatch.csv"})


def _sse(event, data):
    return f"event: {event}\ndata: {json.dumps(jsonable_encoder(data), ensure_ascii=False)}\n\n"


@app.get("/api/stream")
async def stream(request: Request, user=Depends(current_user)):
    async def gen():
        try:
            snap = await run_in_threadpool(fetch_all, "SELECT event_id, event_time, magnitude, place, region, longitude, latitude, depth_km, tsunami, loaded_at FROM events ORDER BY event_time DESC LIMIT 20")
            yield _sse("snapshot", snap)
        except Exception:
            yield _sse("warning", {"message": "reintentando"})
        while not await request.is_disconnected():
            await asyncio.sleep(10)
            yield ": ping\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def _airflow(method, path, **kw):
    try:
        r = requests.request(method, f"{config.AIRFLOW_URL}/api/v1{path}", auth=(config.AIRFLOW_USER, config.AIRFLOW_PASSWORD),
                             timeout=8, **kw)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(502, "Airflow no respondió correctamente") from e


@app.get("/api/pipeline/status")
def pipeline_status(user=Depends(current_user)):
    out = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dags": [],
        "airflow": "ok",
        "storage": "minio",
        "ollama": {"status": "unknown"},
        "duckdb": {"status": "unknown", "latency_ms": None},
        "minio": {"buckets": []},
    }
    for dag_id in ("usgs_ingest", "usgs_transform_quality", "usgs_daily_report", "usgs_backfill"):
        try:
            runs = _airflow("GET", f"/dags/{dag_id}/dagRuns", params={"order_by": "-execution_date", "limit": 5})["dag_runs"]
            last = runs[0] if runs else None
            duration = None
            if last and last.get("start_date") and last.get("end_date"):
                try:
                    s = datetime.fromisoformat(str(last["start_date"]).replace("Z", "+00:00"))
                    e = datetime.fromisoformat(str(last["end_date"]).replace("Z", "+00:00"))
                    duration = round((e - s).total_seconds(), 1)
                except Exception:
                    duration = None
            fails = sum(1 for r in runs if r.get("state") == "failed")
            out["dags"].append({
                "dag_id": dag_id,
                "state": last["state"] if last else None,
                "last_run": last.get("end_date") or last.get("start_date") if last else None,
                "start_date": last.get("start_date") if last else None,
                "end_date": last.get("end_date") if last else None,
                "duration_sec": duration,
                "failures_last5": fails,
                "recent_runs": [{"state": r.get("state"), "execution_date": r.get("execution_date"), "start_date": r.get("start_date"), "end_date": r.get("end_date")} for r in runs]
            })
        except Exception:
            out["airflow"] = "unavailable"
            out["dags"].append({"dag_id": dag_id, "state": "unknown", "last_run": None, "start_date": None, "end_date": None, "duration_sec": None, "failures_last5": 0, "recent_runs": []})
    try:
        t0 = time.time()
        r = requests.get(os.getenv("OLLAMA_URL", "http://ollama:11434") + "/api/version", timeout=3)
        out["ollama"] = {"status": "up" if r.ok else "down", "latency_ms": round((time.time() - t0) * 1000, 1)}
    except Exception:
        out["ollama"] = {"status": "down", "latency_ms": None}
    try:
        t0 = time.time()
        from . import db
        db.fetch_all("SELECT 1 as ok")
        out["duckdb"] = {"status": "up", "latency_ms": round((time.time() - t0) * 1000, 1)}
    except Exception as e:
        out["duckdb"] = {"status": "down", "latency_ms": None, "error": str(e)[:80]}
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minio"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
            region_name="us-east-1",
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )
        buckets = []
        total_objs = 0
        total_bytes = 0
        for b in ["bronze", "silver", "gold"]:
            try:
                objs = s3.list_objects_v2(Bucket=b)
                cnt = objs.get("KeyCount", 0)
                sz = sum((o.get("Size") or 0) for o in objs.get("Contents", []) or [])
                buckets.append({"name": b, "objects": cnt, "size_bytes": sz, "size_mb": round(sz / 1024 / 1024, 3)})
                total_objs += cnt
                total_bytes += sz
            except Exception as ex:
                buckets.append({"name": b, "objects": 0, "size_bytes": 0, "size_mb": 0.0, "error": str(ex)[:60]})
        out["minio"] = {"endpoint": os.getenv("MINIO_ENDPOINT"), "buckets": buckets, "total_objects": total_objs, "total_size_mb": round(total_bytes / 1024 / 1024, 3)}
    except Exception as e:
        out["minio"] = {"buckets": [], "error": str(e)[:80]}
    try:
        out["quality"] = fetch_all("SELECT run_ts, dag_id, rules_evaluated, rules_passed, rules_failed, score, status, checks FROM quality_runs ORDER BY run_ts DESC LIMIT 20")
    except Exception:
        out["quality"] = []
    if user["role"] != "admin":
        out["quality"] = out["quality"][:1]
        for d in out["dags"]:
            d.pop("recent_runs", None)
    return out


@app.get("/api/quality/failures")
def quality_failures(user=Depends(require_admin)):
    try:
        return fetch_all("SELECT run_ts, dag_id, rules_failed, checks FROM quality_runs WHERE status = 'failed' ORDER BY run_ts DESC LIMIT 50")
    except Exception:
        return []


class BackfillIn(BaseModel):
    days: int = Field(30, ge=1, le=90)


class RegenIn(BaseModel):
    report_date: date


@app.post("/api/admin/backfill")
def backfill(body: BackfillIn, user=Depends(require_admin)):
    r = _airflow("POST", "/dags/usgs_backfill/dagRuns", json={"conf": {"days": body.days}})
    return {"dag_run_id": r.get("dag_run_id"), "state": r.get("state")}


@app.post("/api/admin/reports/regenerate")
def regenerate(body: RegenIn, user=Depends(require_admin)):
    r = _airflow("POST", "/dags/usgs_daily_report/dagRuns", json={"conf": {"report_date": body.report_date.isoformat()}})
    return {"dag_run_id": r.get("dag_run_id"), "state": r.get("state")}


class AskIn(BaseModel):
    question: str = Field(min_length=3, max_length=500)


@app.post("/api/agent/ask")
def agent_ask(body: AskIn, user=Depends(current_user)):
    return agent.ask(body.question)


class DuckQuery(BaseModel):
    sql: str = Field(min_length=3, max_length=1000)


@app.post("/api/duck/query")
def duck_query(body: DuckQuery, user=Depends(current_user)):
    sql = body.sql.strip().rstrip(';')
    forbidden = re.compile(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|COPY|CALL|EXEC|MERGE)\b", re.I)
    if forbidden.search(sql) or not re.match(r"^\s*select\b", sql, re.I):
        raise HTTPException(400, "Solo consultas SELECT")
    if "limit" not in sql.lower():
        sql += " LIMIT 100"
    try:
        from . import db
        rows = db.fetch_all(sql)
        return {"rows": rows}
    except Exception as e:
        raise HTTPException(400, str(e))

@app.get("/ws")
async def websocket_endpoint(websocket: Request):
    # simple fallback
    await websocket.accept()
    await websocket.close()


@app.websocket("/ws")
async def ws(ws):
    await ws.accept()
    try:
        while True:
            await ws.send_json({"type": "ping"})
            await asyncio.sleep(30)
    except Exception:
        pass


@app.get("/api/roles")
def roles(user=Depends(current_user)):
    role = user["role"]
    perms = {
        "visitante": {
            "dashboard": False,
            "reports": False,
            "realtime": False,
            "pipeline": False,
            "admin_actions": False},
        "viewer": {
            "dashboard": True,
            "reports": True,
            "export": True,
            "realtime": True,
            "pipeline": True,
            "pipeline_detail": False,
            "admin_actions": False},
        "admin": {
            "dashboard": True,
            "reports": True,
            "export": True,
            "realtime": True,
            "pipeline": True,
            "pipeline_detail": True,
            "admin_actions": True}}
    return {"role": role, "permissions": perms.get(role, perms["visitante"]), "matrix": {
        "visitante": {"dashboard": False,"reports": False,"realtime": False,"pipeline": "No","admin_actions": False},
        "viewer": {"dashboard": True,"reports": "Sí (CSV/PDF)","realtime": True,"pipeline": "Sí (resumen)","admin_actions": False},
        "admin": {"dashboard": True,"reports": "Sí (CSV/PDF)","realtime": True,"pipeline": "Sí (detalle)","admin_actions": True}
    }}

@app.get("/api/events/export/pdf")
def export_events_pdf(min_mag: float | None = Query(None), user=Depends(current_user)):
    if user.get("role") != "admin":
        raise HTTPException(403, "Solo administradores pueden exportar")
    where, p = ["TRUE"], []
    if min_mag is not None:
        where.append("CAST(magnitude AS DOUBLE) >= ?")
        p.append(min_mag)
    try:
        sql = "SELECT event_id, event_time, magnitude, place, region FROM events WHERE " + " AND ".join(where) + " ORDER BY event_time DESC LIMIT 1000"
        rows = fetch_all(sql, tuple(p))
    except Exception:
        rows = []
    html = "<html><head><meta charset='utf-8'><title>SismoWatch</title><style>body{font-family:Arial,Helvetica,sans-serif;margin:40px;color:#1f1b16}h1{color:#8c7a3d}table{width:100%;border-collapse:collapse;margin-top:20px}th{background:#f7f5f0;text-align:left;padding:10px;border:1px solid #e6dfd3}td{padding:8px;border:1px solid #e6dfd3;font-size:12px}</style></head><body><h1>SismoWatch - Reporte de Eventos</h1><p>Generado: " + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC") + " | Total: " + str(len(rows)) + "</p><table><thead><tr><th>ID</th><th>Fecha/Hora</th><th>Magnitud</th><th>Ubicación</th><th>Región</th></tr></thead><tbody>"
    for r in rows:
        html += "<tr><td>" + str(r.get('event_id','')) + "</td><td>" + str(r.get('event_time','')) + "</td><td>" + str(r.get('magnitude','')) + "</td><td>" + str(r.get('place','')) + "</td><td>" + str(r.get('region','')) + "</td></tr>"
    html += "</tbody></table></body></html>"
    return Response(content=html, media_type="text/html; charset=utf-8", headers={"Content-Disposition": "attachment; filename=sismowatch_eventos.html"})


@app.get("/")
def root():
    return {"name": "SismoWatch API (MinIO)", "version": "1.0", "docs": "/docs", "health": "/health"}


@app.get("/api/last-update")
def last_update(user=Depends(current_user)):
    try:
        r = fetch_all("SELECT max(loaded_at) AS last_update FROM events")
        if r and r[0].get("last_update"):
            return {"last_update": r[0]["last_update"]}
        return {"last_update": None}
    except Exception:
        return {"last_update": None}
