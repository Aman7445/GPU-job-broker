FROM docker.io/library/python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    BROKER_DB=/data/broker.sqlite

WORKDIR /app
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt \
    && useradd --uid 10001 --user-group --no-create-home broker \
    && install -d -o broker -g broker /data

COPY job_broker.py replay.py runtime.py sqlite_ops.py ./
COPY broker_core/ broker_core/
COPY data/01a_trace.csv data/01a_trace.csv

USER 10001:10001
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD ["python", "-c", "from urllib.request import urlopen; urlopen('http://127.0.0.1:8080/readyz', timeout=2).close()"]
CMD ["python", "-m", "uvicorn", "runtime:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1", "--timeout-graceful-shutdown", "10", "--no-proxy-headers"]
