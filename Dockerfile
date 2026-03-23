FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY src /app/src
COPY scripts /app/scripts
COPY config.toml /app/config.toml
COPY config.example.toml /app/config.example.toml
COPY channels.json /app/channels.json

RUN pip install --no-cache-dir .

CMD ["python", "-m", "omx_brainstorm.cli", "run-comparison", "--config", "config.toml"]
