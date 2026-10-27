from airflow.datasets import Dataset

BRONZE = Dataset("postgres://sismowatch/bronze/usgs_raw")
