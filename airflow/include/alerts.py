"""on_failure_callback: webhook opcional. NUNCA debe lanzar excepción ni filtrar secretos."""
import logging
import os

import requests

log = logging.getLogger(__name__)


def notify_failure(context):
    try:
        ti = context.get("task_instance")
        msg = f"❌ Airflow: {ti.dag_id}.{ti.task_id} falló (run {context.get('run_id')}) intento {ti.try_number}"
        log.error(msg)
        url = os.getenv("ALERT_WEBHOOK_URL")
        if url:
            requests.post(url, json={"text": msg}, timeout=5)
    except Exception as e:  # noqa: BLE001 - el callback jamás debe romper el scheduler
        log.warning("alerta no enviada: %s", type(e).__name__)
