# Core Service

## Structure overview

- /[routes](src/dag_service/routes) - HTTP routes module
- /[cli.py](src/dag_service/cli.py) - execution entrypoint
- /[config.py](src/dag_service/config.py) - configurations
- /[handlers.py](src/dag_service/handlers.py) - workflow handlers referenced in workflow definitions under `dag.nodes.handler` field
- /[ioc.py](src/dag_service/ioc.py) - simple DI container implementation
- /[server.py](src/dag_service/server.py) - Uvicorn server config and launcher
- /[store.py](src/dag_service/store.py) - Redis persistent storage for storing workflow represented as json

---
## Service components

---

- ### HTTP WEB API + Workflow Orchestrator(WF Orchestrator)

#### Rationale
Decision to merge this to components was made in order to omit interservice communication between HTTP API and orchestator

#### HTTP WEB API component
Component responsible for:
- (HTTP API) Exposing client API for communication with Workflow engine
- (WF Orchestrator) Orchestrator and managing lifecycle of the workflow
- (WF Orchestrator) Recording workflow execution details and result in persistent storage

##### CORE API endpoints
- **`POST`** /api/workflow -  Submit a new workflow JSON for execution, returns execution_id.
- **`POST`** /api/workflow/trigger/:execution_id - Triggers the execution to start.
- **`GET`** /api/workflows/:instance_id - Retrieve the current status of the workflow (Pending, Running, Completed).
- **`GET`** /api/workflows/:instance_id/results - Retrieve the final aggregated output of the workflow.


##### DOCS API endpoints
- **`GET`** /api/docs - Swagger UI.
- **`GET`** /api/openapi.json - OpenAPI schema.

---

- ### Workflow Orchestrator
Component responsible for management of workflow process execution and lifecycle management of the individual workflows as well as spawning new workflow processes and recording process execution data.

##### Features
- **Dependency Resolution:** The Orchestrator determines which nodes are eligible to run (nodes with 0 dependencies or nodes where all parents are COMPLETED).
- **Parallelism:** Independent nodes dispatches simultaneously.
- **State Management(nodes):** The system tracks the state of every node (**PENDING**, **RUNNING**, **COMPLETED**, **FAILED**).
- **State Management(workflow):** The system tracks the state of workflow process (**RUNNING**, **COMPLETED**, **FAILED**).

---

- ### Worker nodes
Component responsible for actual execution of nodes with handlers.
Built using competing consumers pattern principle, to allow horizontal scaling.


Handlers are defined in ```src/dag_service/handlers.py``` file.
