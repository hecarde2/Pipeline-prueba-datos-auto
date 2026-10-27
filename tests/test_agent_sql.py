import pytest
from app.agent import SQLRejected, validate_sql


def test_select_gold_ok():
    assert validate_sql("SELECT region, count(*) FROM gold.events GROUP BY 1;").startswith("SELECT")


@pytest.mark.parametrize("sql", ["DROP TABLE gold.events", "SELECT * FROM silver.events", "SELECT 1; DELETE FROM gold.events",
                                 "SELECT * FROM gold.events -- x", "UPDATE gold.events SET region='x'",
                                 "SELECT pg_sleep(5) FROM gold.events", "SELECT * FROM information_schema.tables", "SELECT 1"])
def test_dangerous_sql_rejected(sql):
    with pytest.raises(SQLRejected):
        validate_sql(sql)
