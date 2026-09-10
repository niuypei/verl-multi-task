"""Shared startup resource registry; scheduling and resource actions remain empty."""

import ray
from ray.actor import ActorHandle

from .registration import (
    RegistrationError,
    TaskResourceRegistration,
    derive_resource_view,
    normalize_registration,
)

RUNTIME_KIND = "verl-multi-task:experimental_fully_async_standalone:registration-v1"


@ray.remote(num_cpus=0)
class GroupScheduler:
    """Keep controller handles and one immutable registration per task.

    The synchronous Actor serializes validation and commit without downstream
    RPCs. Registration does not certify idle GPUs or authorize any sharing.
    """

    def __init__(self) -> None:
        self.task_runners: dict[str, ActorHandle] = {}
        self.task_resources: dict[str, TaskResourceRegistration] = {}

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
        """Remove this task's metadata and handle, without releasing physical GPUs."""
        self.task_runners.pop(task_id, None)
        self.task_resources.pop(task_id, None)

    def register_task_resources(self, registration: TaskResourceRegistration) -> dict[str, str]:
        """Validate an attached task's full topology and commit it exactly once."""
        if type(registration) is not TaskResourceRegistration:
            raise RegistrationError("registration must be a TaskResourceRegistration record")
        task_id = registration.task_id
        if type(task_id) is not str or task_id not in self.task_runners:
            raise RegistrationError("Task must attach its TaskRunner before registering resources")
        normalized = normalize_registration(registration)
        existing = self.task_resources.get(task_id)
        if existing is not None:
            if existing != normalized:
                raise RegistrationError(f"Task topology conflict for {task_id}; registration cannot update resources")
            return {"task_id": task_id, "status": "ALREADY_REGISTERED"}

        # Validate a temporary candidate mapping, never partially update registry
        # or maintain a second independently writable node/GPU ownership index.
        derive_resource_view({**self.task_resources, task_id: normalized})
        self.task_resources[task_id] = normalized
        return {"task_id": task_id, "status": "REGISTERED"}

    def get_task_resources(self, task_id: str) -> TaskResourceRegistration | None:
        """Return a detached immutable copy of a registration, or None."""
        registration = self.task_resources.get(task_id)
        return None if registration is None else normalize_registration(registration)

    def get_resource_view(self) -> dict:
        """Derive fresh metadata-only dictionaries from the registration records."""
        return derive_resource_view(self.task_resources)

    def get_task_runners(self) -> dict[str, ActorHandle]:
        """Return the current reference map for initialization verification."""
        return dict(self.task_runners)

    def schedule(self) -> list:
        """Empty extension: produce no scheduling decisions or resource actions."""
        return []
