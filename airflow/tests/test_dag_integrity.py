from pathlib import Path

import pytest

pytest.importorskip("airflow.models")   # se omite fuera del contenedor de Airflow


def test_all_dags_import_without_errors():
    from airflow.models import DagBag
    bag = DagBag(dag_folder=str(Path(__file__).resolve().parents[1] / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    assert {"usgs_ingest", "usgs_transform_quality", "usgs_daily_report", "usgs_backfill"} <= set(bag.dags)
    for d in bag.dags.values():
        assert d.tags and d.doc_md
