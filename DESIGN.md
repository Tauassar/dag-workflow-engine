## Detecting Node Readiness (Fan-in Logic)

A node becomes ready to execute when:
All of its dependencies have reached COMPLETED
The node itself is in PENDING
The node is not BLOCKED by a failed dependency
Algorithm:
```python
if node.status == PENDING and all(parent.status == COMPLETED for parent in node.depends_on):
    schedule(node)
```

### Benefits:

- Deterministic and simple
- Works for arbitrary DAG shapes
- Supports fan-in (multiple parents)
- Supports independent parallel execution (each ready node is dispatched)

### Alternatives

#### Maintaining ready-queue that updates on every event
- More complex, harder to implement
- Harder to guarantee correctness in partial failures

---

## Handling Fan-In

### Guarantees delivered via:
- DAG stored in memory and modified using locks
- Updates on every result
- Checking parent state before dispatching dependents

### Trade-off:
- Requires orchestrator-local state (not stateless)
- Harder shutdown recovery logic
- Cannot easily be offloaded entirely to persistent storage without complex atomic updates

---

## Timeout Detection

The timeout monitor periodically inspects:
```
    node.started_at + timeout_seconds < now
```
If exceeded, retry attempt is dispatched. If attempts exhausted -> node fails -> dependents get failed

### Trade-off:
- Requires orchestrator-local clock and monitoring loop

### Alternatives
- workers sending heartbeats (more complex)

---

## External Result Storage
Worker execution results are stored in external storage, rather than passing them through message broker

## Benefits:
- Allow pass huge execution results
- High durability
- Easy result aggregation
- Simplifies template resolution
- Can be utilized as additional idempotency flag

### Trade-off:
- Requires orchestrator-local clock and monitoring loop

### Alternatives
- workers sending heartbeats (more complex)

---

## Idempotency Design

Using dedicated dispatch:{workflow_id}:{node_id}:{attempt} and exec:{workflow_id}:{node_id} keys for idempotency check

### Benefits:
- Strong idempotency guarantees
- Simple mechanism

### Trade-off:
- Additional storage

---

## High level working architecture

```psql
+-----+-------------------------+-----++-----+-------------------------+-----+
| Single service       +------------------------+                            |
|                      |        HTTP API        |                            |
|                      |                        |                            |
|                      +------------+-----------+                            |
|                                 |                                          |
|                                 v                                          |
|                      +------------------------+                            |
|                      |  Workflow Definition   |                            |
|                      |      JSON in Redis     |                            |
|                      +------------+-----------+                            |
|                                 |                                          |
|                                 v                                          |
|                   +--------------+-----------------+                       |
|                   |       WorkflowManager          |                       |
|                   |      Orchestrator Spawn        |                       |
|                   +---------+-----------+----------+                       |
|                        |                       |                           |
|                        | dispatch              | receive                   |
|                        v                       v                           |
|                 +-----+-----------+-------------------+                    |
|                 |            DagOrchestrator          |                    |
|                 |  DAG Execution, Scheduling, Retry   |                    |
|                 +-----+-------------------------+-----+                    |
|                        |                         ^                         |
|                        |                         |                         |
+-----+-------------------------+-----++-----+-------------------------+-----+
                         |                         |
                  publish tasks               result queue
                         v                         |
                 +-------+--------+--------++------+------------+
                 |              TransportLayer                  |
                 |                                              |
                 +-------+--------+--------++------+------------+
                         |                         ^
                         v                         |
                 +-------+--------+--------++------+------------+
                 |           Transport Backend                  |
                 |        Redis Streams Pub / Sub               |
                 +-------+--------+--------++------+------------+
                         |                         ^
                         |                         |
                   workers consume          dispatches to orchestrators
                         v                         |
                      +------------------+------------------+
                      |               Worker                |
                      |              (Python)               |
                      +-------------------------------------+
```
