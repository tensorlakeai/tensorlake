from datetime import datetime

from pydantic import BaseModel


class TaskInfoMetadata(BaseModel):
    pending_tasks: int
    successful_tasks: int
    failed_tasks: int


class InvocationMetadata(BaseModel):
    id: str
    completed: bool
    status: str
    outcome: str
    outstanding_tasks: int
    task_analytics: dict[str, TaskInfoMetadata]
    graph_version: str
    created_at: datetime
