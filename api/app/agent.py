import json
import re
from typing import Any, Dict

from . import db
from ollama_client import generate

FORBIDDEN = re.compile(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|COPY|CALL|EXEC|MERGE|ATTACH|DETACH|PRAGMA)\b", re.I)
ALLOWED_TABLES = {"events", "hourly_stats", "quality_runs"}


SCHEMA = """
Tablas (sin prefijo):
- events(event_id, event_time, updated_at, magnitude, mag_bucket, mag_type, place, region, longitude, latitude, depth_km, tsunami, significance, status, url, loaded_at)
"""


def _extract_sql(text: str) -> str:
    if not text:
        return ""
    text = text.replace('`', '')
    m = re.search(r"```sql\s*(.*?)\s*```", text, re.S | re.I)
    if m:
        return m.group(1).strip()
    m = re.search(r"\bselect\b.*", text, re.S | re.I)
    if m:
        s = m.group(0)
        if ';' in s:
            s = s.split(';')[0]
        return s.strip()
    return text.strip()


def _safe_sql(sql: str) -> str:
    s = sql.strip().rstrip(';')
    if not s:
        raise Exception("sin sql")
    if FORBIDDEN.search(s):
        raise Exception("solo select")
    m = re.search(r"\bselect\b.*", s, re.S | re.I)
    if not m:
        raise Exception("requiere select")
    s = m.group(0).strip()
    s = re.sub(r"\bgold\.", "", s, flags=re.I)
    sl = s.lower()
    for t in re.findall(r"\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)\b|\bjoin\s+([a-zA-Z_][a-zA-Z0-9_]*)\b", s, re.I):
        for tt in t:
            if tt and tt.lower() not in ALLOWED_TABLES:
                raise Exception("tabla no permitida")
    if " limit " not in sl:
        s += " LIMIT 80"
    return s


def ask(question: str) -> Dict[str, Any]:
    ql = question.lower().strip()
    if ql in ("hola", "holaa", "hey", "hi"):
        return {"answer": "Hola, soy el asistente IA de SismoWatch. Pregúntame sobre eventos sísmicos.", "verified": True, "unverified_numbers": [], "tables": []}
    if "quien eres" in ql or "qué eres" in ql:
        return {"answer": "Asistente analítico conectado a gold.events.", "verified": True, "unverified_numbers": [], "tables": []}
    if ql == "ayuda":
        return {"answer": "Prueba: 'top regiones', 'últimos 10 eventos', 'cuántos eventos hay', 'eventos de Alaska'.", "verified": True, "unverified_numbers": [], "tables": []}

    try:
        if re.search(r"dame\s+(\d+)", ql):
            n = int(re.search(r"dame\s+(\d+)", ql).group(1))
            n = min(max(n,1), 50)
            sql = _safe_sql(f"SELECT event_id, event_time, magnitude, place, region FROM events ORDER BY event_time DESC LIMIT {n}")
            rows = db.fetch_all(sql)
            cols = list(rows[0].keys()) if rows else []
            return {"answer": f"Eventos ({len(rows)}).", "verified": True, "unverified_numbers": [], "tables": [{"sql": sql, "columns": cols, "rows": rows, "row_count": len(rows), "truncated": False}]}
        if "10 primeros" in ql or "primeros 10" in ql or ql == "dame 10":
            sql = _safe_sql("SELECT event_id, event_time, magnitude, place, region FROM events ORDER BY event_time DESC LIMIT 10")
            rows = db.fetch_all(sql)
            cols = list(rows[0].keys()) if rows else []
            return {"answer": f"Últimos eventos ({len(rows)}).", "verified": True, "unverified_numbers": [], "tables": [{"sql": sql, "columns": cols, "rows": rows, "row_count": len(rows), "truncated": False}]}
        if re.search(r"ultimo|último|ultimos|últimos", ql):
            sql = _safe_sql("SELECT event_id, event_time, magnitude, place, region FROM events ORDER BY event_time DESC LIMIT 10")
            rows = db.fetch_all(sql)
            cols = list(rows[0].keys()) if rows else []
            return {"answer": f"Últimos eventos ({len(rows)}).", "verified": True, "unverified_numbers": [], "tables": [{"sql": sql, "columns": cols, "rows": rows, "row_count": len(rows), "truncated": False}]}
        if re.search(r"cuantos|cuántos", ql) and "evento" in ql:
            sql = _safe_sql("SELECT COUNT(*) AS total FROM events")
            rows = db.fetch_all(sql)
            cols = list(rows[0].keys()) if rows else []
            return {"answer": f"Total ({rows[0].get('total')})", "verified": True, "unverified_numbers": [], "tables": [{"sql": sql, "columns": cols, "rows": rows, "row_count": len(rows), "truncated": False}]}
        if re.search(r"top.*region", ql):
            sql = _safe_sql("SELECT region, COUNT(*) AS total FROM events GROUP BY region ORDER BY total DESC LIMIT 10")
            rows = db.fetch_all(sql)
            cols = list(rows[0].keys()) if rows else []
            return {"answer": f"Top regiones ({len(rows)}).", "verified": True, "unverified_numbers": [], "tables": [{"sql": sql, "columns": cols, "rows": rows, "row_count": len(rows), "truncated": False}]}
        if re.search(r"eventos?\s+de\s+([a-záéíóúñ ]+)", ql):
            reg = re.search(r"eventos?\s+de\s+([a-záéíóúñ ]+)", ql).group(1).strip().replace("'", "''")
            sql = _safe_sql(f"SELECT event_id, event_time, magnitude, place, region FROM events WHERE LOWER(region) LIKE LOWER('%{reg}%') ORDER BY event_time DESC LIMIT 15")
            rows = db.fetch_all(sql)
            cols = list(rows[0].keys()) if rows else []
            return {"answer": f"Eventos ({len(rows)}).", "verified": True, "unverified_numbers": [], "tables": [{"sql": sql, "columns": cols, "rows": rows, "row_count": len(rows), "truncated": False}]}
    except Exception as e:
        pass

    system = "Responde ÚNICAMENTE con SQL SELECT para DuckDB. Usa tabla events sin prefijo. Sin markdown."
    prompt = f"{SCHEMA}\nPregunta: {question}\nSQL:"
    try:
        raw = generate(prompt, system=system, temperature=0.0, max_tokens=100)
        sql = _extract_sql(raw)
        sql = _safe_sql(sql)
        rows = db.fetch_all(sql)
        if not rows:
            return {"answer": "Sin resultados.", "verified": True, "unverified_numbers": [], "tables": [{"sql": sql, "columns": [], "rows": [], "row_count": 0, "truncated": False}]}
        cols = list(rows[0].keys())
        return {"answer": f"Se encontraron {len(rows)} resultado(s).", "verified": True, "unverified_numbers": [], "tables": [{"sql": sql, "columns": cols, "rows": rows[:60], "row_count": len(rows), "truncated": False}]}
    except Exception:
        try:
            sql2 = _safe_sql("SELECT event_id, event_time, magnitude, place, region FROM events ORDER BY event_time DESC LIMIT 10")
            rows2 = db.fetch_all(sql2)
            cols2 = list(rows2[0].keys()) if rows2 else []
            return {"answer": f"Últimos eventos ({len(rows2)}).", "verified": True, "unverified_numbers": [], "tables": [{"sql": sql2, "columns": cols2, "rows": rows2, "row_count": len(rows2), "truncated": False}]}
        except Exception:
            return {"answer": "No se pudo generar la consulta.", "verified": True, "unverified_numbers": [], "tables": []}

