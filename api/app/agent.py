import json
import re
from typing import Any, Dict

from . import db
from ollama_client import generate

FORBIDDEN = re.compile(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|COPY|CALL|EXEC|MERGE|ATTACH|DETACH|PRAGMA)\b", re.I)
ALLOWED_TABLES = {"events", "hourly_stats", "quality_runs", "daily_reports", "anomalies", "ai_runs"}


SCHEMA = """
Tablas gold disponibles:
- events(event_id, event_time, updated_at, magnitude, mag_bucket, mag_type, place, region, longitude, latitude, depth_km, tsunami, significance, status, url, loaded_at)
- hourly_stats(hour, events, max_magnitude, avg_magnitude)
- quality_runs(run_ts, dag_id, rules_evaluated, rules_passed, rules_failed, score, status, checks)
- daily_reports(report_date, headline, summary, highlights, facts, generated_by, model, prompt_version, created_at)
- anomalies(hour, events, baseline, zscore, explanation, detected_at)
- ai_runs(run_ts, task, model, prompt_version, tokens_in, tokens_out, latency_ms, cost_usd, status, error)
"""


def _extract_sql(text: str) -> str:
    if not text:
        return ""
    # try code block
    m = re.search(r"```sql\s*(.*?)\s*```", text, re.S | re.I)
    if m:
        return m.group(1).strip()
    m = re.search(r"```\s*(select.*?)\s*```", text, re.S | re.I)
    if m:
        return m.group(1).strip()
    # look for select
    m = re.search(r"(select\s+.*?)(?:\n\n|$)", text, re.S | re.I)
    if m:
        return m.group(1).strip()
    # last resort
    return text.strip()


def _safe_sql(sql: str) -> str:
    s = sql.strip().rstrip(';')
    if not s:
        raise Exception("Sin SQL")
    if FORBIDDEN.search(s):
        raise Exception("Solo consultas SELECT")
    if not re.match(r"^\s*select\b", s, re.I):
        # try to find select inside
        m = re.search(r"select\b.*", s, re.S | re.I)
        if not m:
            raise Exception("La consulta debe ser SELECT")
        s = m.group(0).strip()
    # check tables
    for t in re.findall(r"\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)\b|\bjoin\s+([a-zA-Z_][a-zA-Z0-9_]*)\b", s, re.I):
        for tt in t:
            if tt and tt.lower() not in ALLOWED_TABLES and tt.lower() not in {"information_schema", "pg_catalog", "pg_type"}:
                raise Exception(f"Tabla no permitida: {tt}")
    sl = s.lower()
    if "limit" not in sl:
        s += " LIMIT 100"
    return s


def ask(question: str) -> Dict[str, Any]:
    ql = question.lower().strip()
    if ql in ("hola", "holaa", "hey", "hi", "buenos dias", "buen dia", "buenas tardes", "buenas noches"):
        return {"answer": "Hola, soy el asistente IA de SismoWatch. Puedo consultar eventos sísmicos en gold.events, regiones, magnitudes y estadísticas. ¿Qué deseas saber?", "verified": True, "unverified_numbers": [], "tables": []}
    if "quien eres" in ql or "quién eres" in ql or "que eres" in ql or "qué eres" in ql:
        return {"answer": "Soy el asistente analítico de SismoWatch. Traduzco tus preguntas a SQL SELECT sobre el lakehouse MinIO + DuckDB (esquema gold).", "verified": True, "unverified_numbers": [], "tables": []}
    if "ayuda" in ql or "help" in ql:
        return {"answer": "Prueba: 'top regiones', 'últimos eventos', 'mayor magnitud', 'total de eventos', 'eventos de Chile' o cualquier consulta sobre gold.", "verified": True, "unverified_numbers": [], "tables": []}
    # fast paths
    try:
        if any(k in ql for k in ["top", "region"]) and "region" in ql:
            sql = "SELECT region, COUNT(*) AS total FROM events GROUP BY region ORDER BY total DESC LIMIT 10"
            sql = _safe_sql(sql)
            rows = db.fetch_all(sql)
            cols = list(rows[0].keys()) if rows else []
            return {"answer": f"Top regiones ({len(rows)}).", "verified": True, "unverified_numbers": [], "tables": [{"sql": sql, "columns": cols, "rows": rows, "row_count": len(rows), "truncated": False}]}
        if any(k in ql for k in ["max", "mayor", "fuerte"]) and "mag" in ql:
            sql = "SELECT event_id, event_time, magnitude, place, region FROM events ORDER BY CAST(magnitude AS DOUBLE) DESC NULLS LAST LIMIT 10"
            sql = _safe_sql(sql)
            rows = db.fetch_all(sql)
            cols = list(rows[0].keys()) if rows else []
            return {"answer": f"Mayor magnitud ({len(rows)}).", "verified": True, "unverified_numbers": [], "tables": [{"sql": sql, "columns": cols, "rows": rows, "row_count": len(rows), "truncated": False}]}
        if any(k in ql for k in ["ultimo", "último", "reciente", "hoy"]):
            sql = "SELECT event_id, event_time, magnitude, place, region FROM events ORDER BY event_time DESC LIMIT 15"
            sql = _safe_sql(sql)
            rows = db.fetch_all(sql)
            cols = list(rows[0].keys()) if rows else []
            return {"answer": f"Últimos eventos ({len(rows)}).", "verified": True, "unverified_numbers": [], "tables": [{"sql": sql, "columns": cols, "rows": rows, "row_count": len(rows), "truncated": False}]}
        if "kpi" in ql or "conteo" in ql or "total" in ql:
            sql = "SELECT COUNT(*) AS total FROM events"
            sql = _safe_sql(sql)
            rows = db.fetch_all(sql)
            cols = list(rows[0].keys()) if rows else []
            return {"answer": f"Total de eventos ({rows[0].get('total') if rows else 0}).", "verified": True, "unverified_numbers": [], "tables": [{"sql": sql, "columns": cols, "rows": rows, "row_count": len(rows), "truncated": False}]}
    except Exception:
        pass
    system = "Traduce a UNA consulta SQL DuckDB. SOLO SELECT. Usa solo gold.events/gold.hourly_stats/gold.quality_runs. Sin markdown. Max 80 filas."
    prompt = f"{SCHEMA}\nPregunta: {question}\nSQL:"
    try:
        raw = generate(prompt, system=system, temperature=0, max_tokens=80)
        sql = _extract_sql(raw)
        sql = _safe_sql(sql)
        rows = db.fetch_all(sql)
        if not rows:
            return {"answer": "Sin resultados.", "verified": True, "unverified_numbers": [], "tables": [{"sql": sql, "columns": [], "rows": [], "row_count": 0, "truncated": False}]}
        cols = list(rows[0].keys())
        return {"answer": f"Se encontraron {len(rows)} resultado(s).", "verified": True, "unverified_numbers": [], "tables": [{"sql": sql, "columns": cols, "rows": rows[:60], "row_count": len(rows), "truncated": len(rows) > 60}]}
    except Exception:
        return {"answer": "No se pudo generar la consulta.", "verified": True, "unverified_numbers": [], "tables": []}
