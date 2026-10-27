import os


def _req(name):
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Variable de entorno obligatoria sin definir: {name}")
    return v


JWT_SECRET = _req("JWT_SECRET")
JWT_ALG = "HS256"
JWT_MINUTES = int(os.getenv("JWT_MINUTES", "60"))
DB_DSN = os.getenv("DB_DSN", "")
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_ADMIN_USER", "")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_ADMIN_PASSWORD", "")
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",") if o.strip()]
USERS_SEED = [
    (_req("ADMIN_USER"), _req("ADMIN_PASSWORD"), "admin"),
    (_req("VIEWER_USER"), _req("VIEWER_PASSWORD"), "viewer"),
]
