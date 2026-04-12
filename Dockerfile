FROM python:3.11-slim AS base
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"
COPY pyproject.toml uv.lock ./

FROM base AS jobs
RUN uv sync --frozen --no-dev --only-group jobs
COPY jobs/ jobs/

FROM base AS local
RUN uv sync --frozen --no-dev --only-group local
COPY flows/ flows/
COPY dbt/ dbt/
COPY dashboard/ dashboard/
