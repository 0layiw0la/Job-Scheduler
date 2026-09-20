FROM python:3.13-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY scheduler ./scheduler
RUN pip install --no-cache-dir . alembic

COPY migrations ./migrations
COPY alembic.ini ./

RUN useradd --create-home --uid 10001 scheduler
USER scheduler

EXPOSE 8000
CMD ["scheduler", "serve", "--host", "0.0.0.0", "--port", "8000"]
