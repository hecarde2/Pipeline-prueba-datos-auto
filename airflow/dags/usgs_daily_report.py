"""Genera reporte diario desde MinIO gold."""
from datetime import date, datetime, timedelta
from airflow.decorators import dag, task
from include import reports
from include.alerts import notify_failure

CONN = "pg_sismowatch"
DAG_ID = "usgs_daily_report"


@dag(dag_id=DAG_ID, schedule="0 1 * * *", start_date=datetime(2024, 1, 1), catchup=False,
     tags=["usgs", "gold", "ia", "minio"], doc_md=__doc__,
     default_args={"retries": 2, "retry_delay": timedelta(minutes=1), "retry_exponential_backoff": True,
                   "execution_timeout": timedelta(minutes=10), "on_failure_callback": notify_failure})
def usgs_daily_report():
    @task()
    def run(params=None):
        day = date.fromisoformat(params["report_date"]) if params and params.get("report_date") else date.today() - timedelta(days=1)
        return reports.build_daily(day)

    run()


usgs_daily_report()
