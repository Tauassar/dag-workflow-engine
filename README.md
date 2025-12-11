# Event-Driven Workflow Orchestration Engine
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

The system accepts JSON-based workflow definitions (Directed Acyclic Graphs), parse dependencies, and
orchestrate the execution of tasks across distributed workers.
The engine supports parallel execution of independent branches, manage complex data
passing between nodes, and handle asynchronous task lifecycles using a message broker.

## Technical Stack
- **Language**: Python 3.13 (Type hinted + checked with MyPy)
- **Database**: In-Memory (Redis)
- **Message** Broker: Redis Streams
- **Framework**: FastAPI + Uvicorn
- **Containerization**: Docker & Docker Compose (all services containerized)
- **Package and Environment management**: UV


## Structure

- [/docker](docker) Dockerfile and docker entrypoint 
- [/src/libs/dag-engine](src/libs/dag-engine) Core engine logic wrapped in library format
- [/src/services/dag-service](src/services/dag-service) Actual HTTP API + Orchestrator + Worker implementation

Full documentation on core library and architecture details can be found in [pyproject.toml](src/libs/dag-engine/pyproject.toml)

Full documentation on Worker nodes and HTTP API implementation can be found in service's [README.md](src/services/dag-service/README.md)


## Settings

Following settings can be set using env variables

### Redis

- REDIS_HOST: str = "localhost"
- REDIS_PORT: int = 6379
- REDIS_DB: int = 0

### HTTP
- API_STR: str = "/api"
- API_HOST: str = "0.0.0.0"
- API_PORT: int = 8000
- API_ENABLE_DOCS: bool = True
- BACKEND_CORS_ORIGINS: list[pd.AnyHttpUrl] | str = []

## Installation

This project uses **UV** as its package and environment management system.  
To run the services locally or in a containerized setup, ensure UV (and optionally Docker) are installed on your machine.

---

## Running with Docker & Docker Compose

The easiest way to get a full local environment—including:

- **HTTP API**
- **Orchestrator service**
- **Two worker nodes**

is through Docker Compose.

From the project root, run:

```bash
  docker compose up --build
```

Once the stack finishes booting, you can access the interactive API documentation at:

http://localhost:8000/api/docs

The compose file automatically handles service-to-service networking, environment setup, and process orchestration.


### Running Locally with UV

This project uses **UV** as its package and environment management system.  
If you do not have UV installed, please follow the official installation guide:

**https://docs.astral.sh/uv/getting-started/installation/**

UV provides isolated Python environments, fast dependency resolution, task running, and reproducible builds.


#### Start the HTTP API
```bash
  uv run dag-service web
```

This launches the FastAPI-powered HTTP entrypoint and Workflow Orchestrator, which exposes workflow management, triggering, and metadata APIs.

#### Start a Worker Node

Worker nodes execute workflow steps.
You can run a single node or multiple nodes depending on your local concurrency requirements:

```bash
  uv run dag-service worker
```

Each worker connects to the orchestrator and dynamically pulls tasks from the queue.

## Basic Commands

#### Running linters + MyPy

     pre-commit run --all-files

#### Running pre-commit linters individually

      pre-commit run {{linter_id}} --all-files

example

      pre-commit run mypy --all-files

linter_id for each individual linter can be found in [.pre-commit-config.yaml](.pre-commit-config.yaml) file

linter configurations are set inside [.pre-commit-config.yaml](.pre-commit-config.yaml) and [pyproject.toml](pyproject.toml) files

#### Running tests with pytest

    uv run pytest

### Docker

Dockerfile and entrypoint can be found in [/docker](docker) folder.
