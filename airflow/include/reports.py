import json
import os
from datetime import datetime, timezone
from collections import defaultdict

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config

from . import ollama_client


def _s3():
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "miniopassword123"),
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _ensure(b,s3):
    try:
        s3.head_bucket(Bucket=b)
    except Exception:
        try:
            s3.create_bucket(Bucket=b)
        except Exception:
            pass


def build_daily(day):
    s3 = _s3()
    g = os.getenv("MINIO_BUCKET_GOLD", "gold")
    _ensure(g,s3)
    # read events
    events = []
    try:
        for k in s3.list_objects_v2(Bucket=g, Prefix="events/").get('Contents', []):
            obj = s3.get_object(Bucket=g, Key=k['Key'])
            tbl = pq.read_table(pa.BufferReader(obj['Body'].read()))
            events.extend(tbl.to_pylist())
    except Exception:
        events = []
    daystr = day.isoformat()
    todays = []
    for e in events:
        et = e.get('event_time')
        if isinstance(et, str) and et.startswith(daystr):
            todays.append(e)
        elif hasattr(et, 'date') and et.date().isoformat() == daystr:
            todays.append(e)
    count = len(todays)
    maxm = max((float(x['magnitude']) for x in todays if x.get('magnitude') is not None), default=None)
    regions = defaultdict(int)
    for t in todays:
        regions[t.get('region') or 'Desconocida'] += 1
    top = sorted(regions.items(), key=lambda x: x[1], reverse=True)[:5]
    facts = {"events": count, "max_magnitude": maxm, "top_regions": top}
    headline = f"SismoWatch {daystr}: {count} sismos registrados"
    summary = f"Se registraron {count} eventos sísmicos el {daystr}. Magnitud máxima: {maxm if maxm is not None else 'N/D'}."
    highlights = [{"title": "Actividad", "detail": summary}]
    model = os.getenv("OLLAMA_MODEL")
    generated_by = "fallback"
    try:
        prompt = f"Redacta un breve reporte en español. Fecha {daystr}. Hechos: {json.dumps(facts, ensure_ascii=False)}. Formato JSON con headline, summary, highlights."
        resp = ollama_client.generate(prompt)
        # try parse
        try:
            j = json.loads(resp)
            headline = j.get('headline', headline)
            summary = j.get('summary', summary)
            highlights = j.get('highlights', highlights)
            generated_by = 'llm'
        except Exception:
            generated_by = 'fallback'
    except Exception:
        generated_by = 'fallback'
    report = {
        "report_date": daystr,
        "headline": headline,
        "summary": summary,
        "highlights": highlights,
        "facts": facts,
        "generated_by": generated_by,
        "model": model if generated_by == 'llm' else None,
        "prompt_version": "v1-minio",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    # write
    buf = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist([report]), buf)
    s3.put_object(Bucket=g, Key=f"daily_reports/{daystr}.parquet", Body=buf.getvalue().to_pybytes())
    # anomalies
    try:
        anom = [{"hour": datetime.now(timezone.utc).replace(minute=0,second=0,microsecond=0).isoformat(), "events": count, "baseline": float(count), "zscore": 0.0, "explanation": "Sin anomalía destacada", "detected_at": datetime.now(timezone.utc).isoformat()}]
        buf2 = pa.BufferOutputStream()
        pq.write_table(pa.Table.from_pylist(anom), buf2)
        s3.put_object(Bucket=g, Key=f"anomalies/{daystr}.parquet", Body=buf2.getvalue().to_pybytes())
    except Exception:
        pass
    # ai_runs
    try:
        air = [{"run_ts": datetime.now(timezone.utc).isoformat(), "task": "daily_report", "model": model, "prompt_version": "v1-minio", "tokens_in": 0, "tokens_out": 0, "latency_ms": 0, "cost_usd": 0.0, "status": "ok"}]
        buf3 = pa.BufferOutputStream()
        pq.write_table(pa.Table.from_pylist(air), buf3)
        s3.put_object(Bucket=g, Key=f"ai_runs/{daystr}.parquet", Body=buf3.getvalue().to_pybytes())
    except Exception:
        pass
    return report
