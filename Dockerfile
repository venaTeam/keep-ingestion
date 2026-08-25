FROM python:3.11.6-slim as base

ENV PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Creating a virtual environment and installing dependencies
ENV PIP_DEFAULT_TIMEOUT=100 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VERSION=1.3.2 \
    PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring

RUN pip install "poetry==$POETRY_VERSION"
RUN python -m venv /venv
COPY pyproject.toml poetry.lock ./
RUN . /venv/bin/activate && poetry install --no-root

# Setting the virtual environment path
ENV PYTHONPATH="/app:${PYTHONPATH}"
ENV PATH="/venv/bin:${PATH}"
ENV VIRTUAL_ENV="/venv"

ENV PROMETHEUS_MULTIPROC_DIR="/tmp/prometheus"

# keep-api-gateway is the single owner of the database schema. This image ships
# no alembic.ini and no migrations directory, and `on_starting` waits for the
# gateway's `alembic upgrade head` to settle rather than running one. The env var
# is a second guard, so a mistaken call cannot touch the schema.
ENV SKIP_DB_CREATION=true

# Copy application code
COPY src /app/src

# `-c src/config/config.py` is load-bearing: it wires the gunicorn hooks that
# manage the Prometheus multiprocess directory (`on_starting` clears it before
# workers fork, `child_exit` reaps a dead worker's mmap files). Dropping the flag
# leaves the ingestion counters reporting stale values from dead PIDs.
CMD ["gunicorn", "src.main:get_app", "--bind" , "0.0.0.0:8080" , "--workers", "1" , "-k" , "uvicorn.workers.UvicornWorker", "-c", "src/config/config.py"]
