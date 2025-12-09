import enum


class WorkflowEventType(enum.StrEnum):
    NODE_STARTED   = "NODE_STARTED"
    NODE_SUCCEEDED = "NODE_SUCCEEDED"
    NODE_FAILED    = "NODE_FAILED"
    NODE_SKIPPED   = "NODE_SKIPPED"
    NODE_RETRY     = "NODE_RETRY"
