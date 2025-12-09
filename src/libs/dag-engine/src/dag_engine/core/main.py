from __future__ import annotations
import json

import asyncio, time

from .workflow import WorkflowDAG


USER_JSON = """{
  "name": "Parallel API Fetcher",
  "dag": {
    "nodes": [
      {
        "id": "input",
        "handler": "input",
        "dependencies": []
      },
      {
        "id": "get_user",
        "handler": "call_external_service",
        "dependencies": ["input"],
        "config": {
          "url": "http://localhost:8911/document/policy/list"
        }
      },
      {
        "id": "get_posts",
        "handler": "call_external_service",
        "dependencies": ["input"],
        "config": {
          "url": "http://localhost:8911/document/policy/list"
        }
      },
      {
        "id": "get_comments",
        "handler": "call_external_service",
        "dependencies": ["input"],
        "config": {
          "url": "http://localhost:8911/document/policy/list"
        }
      },
      {
        "id": "output",
        "handler": "output",
        "dependencies": [
          "get_user",
          "get_posts",
          "get_comments"
        ]
      }
    ]
  }
}"""

dag = WorkflowDAG.from_dict(json.loads(USER_JSON), concurrency=3)

@dag.handler("input")
async def input_handler(ctx):
    # produce initial payload (e.g., user id)
    await asyncio.sleep(0.01)
    return {"input_payload": {"user_id": "u-123"}}

@dag.handler("call_external_service")
async def call_external_service(ctx):
    # mock external call using node config
    await asyncio.sleep(0.05)  # simulate latency
    node_def = ctx["node_def"]
    # echo back the url as 'data' for demo
    return {"node": ctx["node_id"], "url": node_def.config.get("url"), "fetched_at": time.time()}

@dag.handler("output")
async def output_handler(ctx):
    """
    Aggregate results of all dependency nodes by reading the DAG state.
    """
    dag = ctx["dag_ref"]
    node = ctx["node_def"]

    dependency_results = {}
    for dep_id in node.depends_on:
        dep_node = dag._nodes[dep_id]
        dependency_results[dep_id] = dep_node.result

    await asyncio.sleep(0.01)  # simulate light processing

    return {
        "aggregated": True,
        "node": node.id,
        "dependencies": dependency_results,
        "timestamp": time.time(),
    }


# --- Run the workflow ---
async def main():
    results = await dag.execute(initial_payload={"run_meta": "demo_run"})
    print("=== results ===")
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
