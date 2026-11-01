FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 TZ=UTC
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY scanner ./scanner
COPY config ./config
RUN useradd -r -u 10001 scanner && mkdir -p /app/logs && chown scanner /app/logs
USER scanner
ENTRYPOINT ["python", "-m", "scanner"]
