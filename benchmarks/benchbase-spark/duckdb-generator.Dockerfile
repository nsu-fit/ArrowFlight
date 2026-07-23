FROM python:3.11-slim

ARG DUCKDB_PYTHON_VERSION=1.4.1

RUN pip install --no-cache-dir --retries 10 --timeout 60 \
    "duckdb==${DUCKDB_PYTHON_VERSION}"

ENTRYPOINT ["python", "/generate-duckdb-data.py"]
