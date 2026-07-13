FROM python:3.11-slim

RUN pip install --no-cache-dir duckdb

COPY generate-duckdb-data.py /generate-duckdb-data.py

ENTRYPOINT ["python", "/generate-duckdb-data.py"]
