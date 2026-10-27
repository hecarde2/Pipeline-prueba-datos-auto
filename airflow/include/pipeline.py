"""Lógica del pipeline sobre MinIO (bronze/silver/gold en parquet)."""
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config

from . import quality, usgs_client
from .transform import feature_to_silver_row

log = logging.getLogger(__name__)


class BatchRejected(Exception):
    pass


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


def _ensure(b, s3):
    try:
        s3.head_bucket(Bucket=b)
    except Exception:
        try:
            s3.create_bucket(Bucket=b)
        except Exception:
            pass


def _key(prefix, name):
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}/{ts}_{name}.parquet"


# BRONZE
def store_bronze_s3(item):
    s3 = _s3()
    b = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
    _ensure(b, s3)
    # store as json for raw + also keep metadata
    key = f"raw/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/{item['sha256']}.json"
    try:
        s3.head_object(Bucket=b, Key=key)
        return None  # exists
    except Exception:
        pass
    s3.put_object(Bucket=b, Key=key, Body=json.dumps(item).encode("utf-8"), ContentType="application/json")
    # write a manifest parquet
    df = pa.table({
        "source": [item["source"]],
        "window_start": [item["window_start"].isoformat() if item["window_start"] else None],
        "window_end": [item["window_end"].isoformat() if item["window_end"] else None],
        "expected_count": [item["expected"]],
        "received_count": [item["received"]],
        "sha256": [item["sha256"]],
        "fetched_at": [datetime.now(timezone.utc).isoformat()],
        "status": ["pending"],
        "key": [key],
    })
    buf = pa.BufferOutputStream()
    pq.write_table(df, buf)
    mkey = f"manifest/{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{item['sha256'][:8]}.parquet"
    s3.put_object(Bucket=b, Key=mkey, Body=buf.getvalue().to_pybytes())
    return mkey


def ingest_feed():
    item = usgs_client.fetch_feed()
    bid = store_bronze_s3(item)
    return {"bronze_id": bid, "expected": item["expected"], "received": item["received"], "duplicate": bid is None}


def ingest_window(start, end):
    items = usgs_client.fetch_window(start, end)
    ids = []
    for it in items:
        ids.append(store_bronze_s3(it))
    return {"bronze_ids": [i for i in ids if i], "count": len([i for i in ids if i])}


def verify_bronze(bid):
    if not bid:
        return {"ok": True, "duplicate": True}
    return {"ok": True}


# SILVER/GOLD
def _write_parquet(bucket, prefix, rows, schema=None):
    s3 = _s3()
    _ensure(bucket, s3)
    if not rows:
        return
    # convert to pa table
    if isinstance(rows[0], dict):
        # build from dicts
        table = pa.Table.from_pylist(rows)
    else:
        table = pa.table(rows)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf)
    key = _key(prefix, "batch")
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().to_pybytes())
    return key


def load_silver():
    s3 = _s3()
    b = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
    s = os.getenv("MINIO_BUCKET_SILVER", "silver")
    _ensure(b,s3); _ensure(s,s3)
    # read pending manifests
    manifests = []
    for k in s3.list_objects_v2(Bucket=b, Prefix="manifest/").get('Contents', []):
        manifests.append(k['Key'])
    processed = []
    failed = []
    for mk in manifests:
        try:
            obj = s3.get_object(Bucket=b, Key=mk)
            tbl = pq.read_table(pa.BufferReader(obj['Body'].read()))
            rows = tbl.to_pylist()
            if not rows or rows[0].get('status') != 'pending':
                continue
            rkey = rows[0]['key']
            raw = json.loads(s3.get_object(Bucket=b, Key=rkey)['Body'].read())
            payload = raw['payload']
            feats = payload.get('features', [])
            silver_rows = []
            for f in feats:
                try:
                    silver_rows.append(feature_to_silver_row(f, mk))
                except Exception as e:
                    raise
            # write silver
            _write_parquet(s, "events", silver_rows)
            # update manifest to processed
            rows[0]['status'] = 'processed'
            rows[0]['processed_at'] = datetime.now(timezone.utc).isoformat()
            t2 = pa.Table.from_pylist(rows)
            buf = pa.BufferOutputStream(); pq.write_table(t2, buf)
            s3.put_object(Bucket=b, Key=mk, Body=buf.getvalue().to_pybytes())
            processed.append(mk)
        except BatchRejected as e:
            failed.append({"bronze_id": mk, "reason": str(e)[:500]})
            try:
                obj = s3.get_object(Bucket=b, Key=mk)
                tbl = pq.read_table(pa.BufferReader(obj['Body'].read()))
                rows = tbl.to_pylist()
                rows[0]['status'] = 'failed'
                rows[0]['error'] = str(e)[:1000]
                t2 = pa.Table.from_pylist(rows)
                buf = pa.BufferOutputStream(); pq.write_table(t2, buf)
                s3.put_object(Bucket=b, Key=mk, Body=buf.getvalue().to_pybytes())
            except Exception:
                pass
            log.error("bronze %s RECHAZADO: %s", mk, e)
        except Exception as e:
            failed.append({"bronze_id": mk, "reason": str(e)[:500]})
            try:
                obj = s3.get_object(Bucket=b, Key=mk)
                tbl = pq.read_table(pa.BufferReader(obj['Body'].read()))
                rows = tbl.to_pylist()
                rows[0]['status'] = 'failed'
                rows[0]['error'] = str(e)[:1000]
                t2 = pa.Table.from_pylist(rows)
                buf = pa.BufferOutputStream(); pq.write_table(t2, buf)
                s3.put_object(Bucket=b, Key=mk, Body=buf.getvalue().to_pybytes())
            except Exception:
                pass
    return {"processed": processed, "failed": failed}


def publish_gold(bronze_ids=None):
    s3 = _s3()
    sv = os.getenv("MINIO_BUCKET_SILVER", "silver")
    g = os.getenv("MINIO_BUCKET_GOLD", "gold")
    _ensure(sv,s3); _ensure(g,s3)
    # collect all silver events
    events = []
    for k in s3.list_objects_v2(Bucket=sv, Prefix="events/").get('Contents', []):
        obj = s3.get_object(Bucket=sv, Key=k['Key'])
        tbl = pq.read_table(pa.BufferReader(obj['Body'].read()))
        events.extend(tbl.to_pylist())
    if not events:
        return {"published": 0}
    gold_rows = []
    for e in events:
        mag = e.get('magnitude')
        if mag is None:
            mb = 'sin dato'
        elif mag < 2:
            mb = '<2'
        elif mag < 3:
            mb = '2-3'
        elif mag < 4:
            mb = '3-4'
        elif mag < 5:
            mb = '4-5'
        else:
            mb = '5+'
        place = e.get('place') or ''
        # extract region as last token after comma
        region = 'Desconocida'
        if ',' in place:
            region = place.split(',')[-1].strip() or 'Desconocida'
        gold_rows.append({
            "event_id": e['event_id'],
            "event_time": e['event_time'],
            "updated_at": e['updated_at'],
            "magnitude": mag,
            "mag_bucket": mb,
            "mag_type": e.get('mag_type'),
            "place": place,
            "region": region,
            "longitude": e['longitude'],
            "latitude": e['latitude'],
            "depth_km": e.get('depth_km'),
            "tsunami": e.get('tsunami'),
            "significance": e.get('significance'),
            "status": e.get('status'),
            "url": e.get('url'),
            "loaded_at": datetime.now(timezone.utc).isoformat(),
        })
    _write_parquet(g, "events", gold_rows)
    # hourly stats
    # simple
    from collections import defaultdict
    agg = defaultdict(lambda: {'events':0,'maxm':None,'sum':0.0,'cnt':0})
    for gr in gold_rows:
        try:
            dt = gr['event_time']
            if isinstance(dt, str):
                # keep hour key
                keyh = dt[:13] + ':00:00+00:00'
            else:
                keyh = dt.replace(minute=0,second=0,microsecond=0).isoformat()
            agg[keyh]['events'] += 1
            if gr['magnitude'] is not None:
                agg[keyh]['cnt'] += 1
                agg[keyh]['sum'] += float(gr['magnitude'])
                if agg[keyh]['maxm'] is None or float(gr['magnitude']) > agg[keyh]['maxm']:
                    agg[keyh]['maxm'] = float(gr['magnitude'])
        except Exception:
            continue
    hs = []
    for k,v in agg.items():
        hs.append({
            "hour": k,
            "events": v['events'],
            "max_magnitude": v['maxm'],
            "avg_magnitude": round(v['sum']/v['cnt'], 2) if v['cnt'] else None,
        })
    _write_parquet(g, "hourly_stats", hs)
    return {"published": len(gold_rows)}


def final_checks(summary=None):
    # write quality run summary
    s3 = _s3()
    g = os.getenv("MINIO_BUCKET_GOLD", "gold")
    _ensure(g,s3)
    qr = [{
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "dag_id": os.getenv("DAG_ID","unknown"),
        "rules_evaluated": 1,
        "rules_passed": 1 if not (summary or {}).get('failed') else 0,
        "rules_failed": len((summary or {}).get('failed',[])),
        "score": 100.0 if not (summary or {}).get('failed') else 0.0,
        "status": "passed" if not (summary or {}).get('failed') else "failed",
        "checks": json.dumps(summary or {}),
    }]
    _write_parquet(g, "quality_runs", qr)
    if (summary or {}).get("failed"):
        raise RuntimeError(f"Calidad: lotes rechazados")
    return {"ok": True}
