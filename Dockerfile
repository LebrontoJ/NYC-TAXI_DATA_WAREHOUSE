FROM apache/airflow:2.9.3-python3.11

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11

COPY requirements.txt /tmp/requirements.txt
COPY dbt-requirements.txt /tmp/dbt-requirements.txt

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    -r /tmp/requirements.txt

RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/dbt-requirements.txt
