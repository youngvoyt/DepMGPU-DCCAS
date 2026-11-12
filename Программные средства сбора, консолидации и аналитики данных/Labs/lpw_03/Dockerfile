FROM apache/airflow:2.5.0-python3.8
# All added dependencies use wheels or pure Python; no OS compiler is needed.
# Avoid obsolete Bullseye/PGDG apt repositories in this historical base image.
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.8.txt
