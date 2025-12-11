# DAG Workflow Engine

A high-performance, event-driven **Directed Acyclic Graph (DAG) Workflow Engine** designed for microservices, orchestration, distributed workers, and long-running asynchronous tasks.

The engine executes workflows defined as JSON-based DAGs, dispatches tasks to workers, processes results through Redis Streams, and persists execution history using event sourcing. It includes fault-tolerance, retries, idempotency, template resolution, and dynamic data passing between nodes.

---

## Features

### General
- Per-node template interpolation
- Failed node propagation
- Event store for auditing
- Isolation per workflow (consumer group per workflow)
- Global result listener (fan-out architecture)

### Workflow Definition & Execution
- JSON-defined workflows
- DAG validation (acyclic, dependency correctness)
- Node types: service call, function, input, output, custom handlers
- Runtime execution via an asynchronous Orchestrator

### Distributed Execution
- Workers pull tasks from Redis Streams
- Orchestrator pushes tasks and consumes results
- Retry logic built-in. 
- Idempotency on worker nodes guaranteed by Redis keys: `exec:{workflow_id}:{node_id}`
- Idempotency orchestrator guaranteed by Redis keys composed key `dispatch:{self.dag.workflow_id}:{node.id}:{node.attempt}`
- Authoritative timeout monitoring for long-running tasks

### Template Resolution
Dynamic interpolation of upstream results:

```json
{
  "config": {
    "url": "http://service/api/{{get_user.user.id}}"
  }
}
```

Templates automatically resolve using `TemplateResolver` before tasks are dispatched.


### Event Sourcing (Optional)
- Append-only workflow events (NODE_STARTED, NODE_COMPLETED, etc.)
- Timeline reconstruction for observability and debugging
- Integrates with Redis but can be extended further through abstraction EventStore to accept various backends

### Persistent Storage
- WorkflowDefinitionStore: stores workflow definitions
- WorkflowExecutionStore: persists execution metadata and results
- Survives restarts, supports querying historical executions

### High Reliability
- Per-node retry policies (fixed/backoff), defined in json definition
- TimeoutMonitor to detect stalled tasks
- Blocked node propagation on failures
- Idempotent task dispatch and result consumption

### Scalable Architecture
- Multiple workers per workflow

### Development
- Implemented local alternatives for redis store/transport implementations for development and local testing purposes 


## Structure overview & explanation

```psql
│   dag_engine
├── __init__.py
├── core                        # core logic module
│   ├── __init__.py
│   ├── constants.py
│   ├── entities.py
│   ├── exceptions.py
│   ├── handlers.py             # handlers registry and typing abstraction 
│   ├── manager.py              # utility to manage multiple Orchestrator executions, launches, builds and records individual workflow execution in persistant storage
│   ├── orchestrator.py         # main Orchestration logic, starts and manages full workflow execution lyfecycle, retries failed nodes and handles results
│   ├── schemas.py
│   ├── templates.py            # templates resolution logic, identifies and parses templates inside strings and injects values from context
│   ├── timeout_monitor.py      # authoritative timeouts implementation
│   ├── worker.py               # worker node implementation, handles tasks, publishes results
│   └── workflow.py             # DAG workflow implementation, validates, creates and manipulates DAG
├── event_sourcing              # encapsulated event_sourcing logic
│   ├── __init__.py
│   ├── constants.py
│   └── schemas.py
├── serializer                  # encapsulated json serialization logic, used in templates resolution module and results serialization
│   ├── __init__.py
│   ├── encoders.py
│   ├── json.py
│   └── protocol.py
├── store                       # persistent storage module, includes storage logic for events, execution details, idempotency keys and execution results
│   ├── __init__.py
│   ├── events
│   │   ├── __init__.py
│   │   ├── local.py
│   │   ├── protocols.py
│   │   └── redis.py
│   ├── execution
│   │   ├── __init__.py
│   │   ├── protocols.py
│   │   └── redis.py
│   ├── idempotency
│   │   ├── __init__.py
│   │   ├── local.py
│   │   ├── protocols.py
│   │   └── redis.py
│   └── results
│       ├── __init__.py
│       ├── protocol.py
│       └── redis.py
└── transport                   # transport layer abstraction
    ├── __init__.py
    ├── local.py
    ├── messages.py
    ├── protocols.py
    └── redis.py
```
