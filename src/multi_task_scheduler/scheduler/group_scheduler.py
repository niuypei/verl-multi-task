"""The shared Actor exists; resource scheduling remains unimplemented."""

import ray
from ray.actor import ActorHandle

RUNTIME_KIND = "verl-multi-task:experimental_fully_async_standalone:p1"


@ray.remote(num_cpus=0)
class GroupScheduler:
    """Keep TaskRunner handles without creating a second resource view.

    The handle map establishes the controller references required by startup.
    It is not a resource registry, heartbeat monitor or scheduling policy.
    """

    def __init__(self) -> None:
        self.task_runners: dict[str, ActorHandle] = {}

    def runtime_kind(self) -> str:
        """Identify this implementation when a job discovers a named Actor."""
        return RUNTIME_KIND

    def attach_task(self, task_id: str, task_runner: ActorHandle) -> None:
        """Save the caller's real ActorHandle; do not register any GPU resources."""
        if not isinstance(task_id, str) or not task_id:
            raise ValueError("task_id must be a nonempty TaskRunner Actor ID")
        if not isinstance(task_runner, ActorHandle):
            raise TypeError("GroupScheduler requires a real TaskRunner ActorHandle")
        existing = self.task_runners.get(task_id)
        if existing is not None and existing != task_runner:
            raise ValueError(f"TaskRunner reference already exists for {task_id}")
        self.task_runners[task_id] = task_runner

    def detach_task(self, task_id: str) -> None:
        """Release a completed TaskRunner reference without changing resources."""
        self.task_runners.pop(task_id, None)

    def get_task_runners(self) -> dict[str, ActorHandle]:
        """Return the current reference map for initialization verification."""
        return dict(self.task_runners)

    def schedule(self) -> list:
        """Empty extension: produce no scheduling decisions or resource actions."""
        return []
