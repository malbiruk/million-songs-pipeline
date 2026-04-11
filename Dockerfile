FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies first (cached layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy project
COPY flows/ flows/
COPY dbt/ dbt/
COPY dashboard/ dashboard/

ENV PATH="/app/.venv/bin:$PATH"
