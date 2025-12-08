# Event-Driven Workflow Orchestration Engine
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

The system accepts JSON-based workflow definitions (Directed Acyclic Graphs), parse dependencies, and
orchestrate the execution of tasks across distributed workers.
The engine supports parallel execution of independent branches, manage complex data
passing between nodes, and handle asynchronous task lifecycles using a message broker.


## Settings

Moved to [settings](https://cookiecutter-django.readthedocs.io/en/latest/1-getting-started/settings.html).

## Basic Commands

### Type checks

Running type checks with mypy:

    uv run mypy fil_management_project

### Test coverage

To run the tests, check your test coverage, and generate an HTML coverage report:

    uv run coverage run -m pytest
    uv run coverage html
    uv run open htmlcov/index.html

#### Running tests with pytest

    uv run pytest

### Docker

See detailed [cookiecutter-django Docker documentation](https://cookiecutter-django.readthedocs.io/en/latest/3-deployment/deployment-with-docker.html).
