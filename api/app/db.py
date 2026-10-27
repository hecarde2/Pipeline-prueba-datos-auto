import os
from typing import Any, Dict, List

import duckdb

class DBUnavailable(Exception):
    pass


def _con():
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace('http://','').replace('https://','')
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    con.execute(f"SET s3_access_key_id='{os.getenv('MINIO_ROOT_USER','minioadmin')}';")
    con.execute(f"SET s3_secret_access_key='{os.getenv('MINIO_ROOT_PASSWORD','miniopassword123')}';")
    return con


def fetch_all(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    try:
        con = _con()
        bucket = os.getenv("MINIO_BUCKET_GOLD", "gold")
        # try to create views
        for name in ['events','hourly_stats','quality_runs','daily_reports','anomalies','ai_runs']:
            try:
                con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('s3://{bucket}/{name}/*.parquet');")
            except Exception:
                try:
                    con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('s3://{bucket}/{name}/**/*.parquet');")
                except Exception:
                    pass
        res = con.execute(sql, params).fetchall()
        if not con.description:
            con.close()
            return []
        cols = [d[0] for d in con.description]
        out = []
        for r in res:
            d = dict(zip(cols, r))
            out.append(d)
        con.close()
        return out
    except Exception as e:
        raise DBUnavailable(str(e))
