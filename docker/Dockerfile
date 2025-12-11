FROM python:3.13-slim AS python-base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=0 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    APPLICATION_PATH="/usr/src" \
    PYTHONPATH="/usr/src" \
    UV_SYSTEM_PYTHON=1 \
    UV_PROJECT_ENVIRONMENT="/opt/venv" \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

ENV PATH="/opt/venv/bin:$PATH"

WORKDIR $APPLICATION_PATH


FROM python-base AS builder

COPY --from=ghcr.io/astral-sh/uv:0.7.20 /uv /uvx /bin/

# Install ONLY production dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-install-workspace --no-dev

# Copy project source code
COPY . .

# Sync project into venv (no dev deps)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-editable --no-dev --all-packages

COPY docker/entrypoint.sh ./
RUN chmod +x entrypoint.sh



FROM python-base AS prod

# Copy the virtual environment built in "builder"
COPY --from=builder /opt/venv /opt/venv

# Copy only application code
COPY --from=builder /usr/src /usr/src

# Entrypoint (can be overridden)
CMD ["./entrypoint.sh"]
