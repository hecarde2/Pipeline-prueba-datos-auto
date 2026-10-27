import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for p in (ROOT / "airflow", ROOT / "shared", ROOT / "api"):
    sys.path.insert(0, str(p))
# Variables mínimas para importar la API en pruebas (no son secretos reales)
for k, v in {"JWT_SECRET": "test-secret-test-secret-test-secret", "ADMIN_USER": "admin", "ADMIN_PASSWORD": "adminpw",
             "VIEWER_USER": "viewer", "VIEWER_PASSWORD": "viewerpw", "DB_DSN": "postgresql://x:x@localhost:1/x"}.items():
    os.environ.setdefault(k, v)
