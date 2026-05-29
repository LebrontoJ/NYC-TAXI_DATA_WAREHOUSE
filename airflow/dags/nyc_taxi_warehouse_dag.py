from __future__ import annotations

from datetime import datetime
from pathlib import Path

import requests
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


PROJECT_DIR = Path("/opt/airflow/project/dbt")
DATA_DIR = Path("/opt/airflow/data/nyc_taxi")
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
MONTHS = ["2024-01", "2024-02", "2024-03"]
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


@dag(
    dag_id="nyc_taxi_warehouse",
    start_date=datetime(2024, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["snowflake", "dbt", "warehouse", "portfolio"],
)
def nyc_taxi_warehouse():
    @task
    def ensure_snowflake_objects() -> None:
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        hook.run(
            """
            CREATE DATABASE IF NOT EXISTS NYC_TAXI_DW;
            CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DW.RAW;
            CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DW.TRANSFORM;
            CREATE SCHEMA IF NOT EXISTS NYC_TAXI_DW.MARTS;

            CREATE FILE FORMAT IF NOT EXISTS NYC_TAXI_DW.RAW.PARQUET_FORMAT
                TYPE = PARQUET
                USE_VECTORIZED_SCANNER = TRUE;

            CREATE STAGE IF NOT EXISTS NYC_TAXI_DW.RAW.NYC_TAXI_STAGE
                FILE_FORMAT = NYC_TAXI_DW.RAW.PARQUET_FORMAT;

            CREATE TABLE IF NOT EXISTS NYC_TAXI_DW.RAW.YELLOW_TAXI_TRIPS (
                src VARIANT,
                source_file STRING,
                source_row_number NUMBER,
                loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            );

            ALTER TABLE NYC_TAXI_DW.RAW.YELLOW_TAXI_TRIPS
                ADD COLUMN IF NOT EXISTS source_row_number NUMBER;
            """
        )

    @task
    def download_trip_files() -> list[str]:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        downloaded_files = []

        for month in MONTHS:
            file_name = f"yellow_tripdata_{month}.parquet"
            target = DATA_DIR / file_name
            if not target.exists():
                response = requests.get(f"{BASE_URL}/{file_name}", timeout=120)
                response.raise_for_status()
                target.write_bytes(response.content)
            downloaded_files.append(str(target))

        return downloaded_files

    @task
    def download_zone_lookup_seed() -> str:
        seed_path = PROJECT_DIR / "seeds" / "taxi_zone_lookup.csv"
        seed_path.parent.mkdir(parents=True, exist_ok=True)
        response = requests.get(ZONE_LOOKUP_URL, timeout=60)
        response.raise_for_status()
        seed_path.write_bytes(response.content)
        return str(seed_path)

    @task
    def upload_and_copy_to_raw(files: list[str]) -> None:
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        for file_path in files:
            hook.run(
                f"""
                PUT 'file://{file_path}' @NYC_TAXI_DW.RAW.NYC_TAXI_STAGE/yellow
                    AUTO_COMPRESS = FALSE
                    OVERWRITE = TRUE;
                """
            )

        hook.run(
            """
            TRUNCATE TABLE NYC_TAXI_DW.RAW.YELLOW_TAXI_TRIPS;

            COPY INTO NYC_TAXI_DW.RAW.YELLOW_TAXI_TRIPS (src, source_file, source_row_number)
            FROM (
                SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
                FROM @NYC_TAXI_DW.RAW.NYC_TAXI_STAGE/yellow
            )
            FILE_FORMAT = (FORMAT_NAME = NYC_TAXI_DW.RAW.PARQUET_FORMAT)
            ON_ERROR = CONTINUE;
            """
        )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {PROJECT_DIR} && {DBT_BIN} deps --profiles-dir .",
    )
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {PROJECT_DIR} && {DBT_BIN} seed --profiles-dir .",
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {PROJECT_DIR} && {DBT_BIN} run --profiles-dir .",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {PROJECT_DIR} && {DBT_BIN} test --profiles-dir .",
    )

    snowflake_ready = ensure_snowflake_objects()
    downloaded = download_trip_files()
    zone_lookup = download_zone_lookup_seed()
    loaded = upload_and_copy_to_raw(downloaded)

    snowflake_ready >> downloaded >> loaded >> dbt_deps
    zone_lookup >> dbt_seed
    dbt_deps >> dbt_seed >> dbt_run >> dbt_test


nyc_taxi_warehouse()
