"""Selected CPU Ray tests: GS discovery, handles and test-only subclass creation.

These tests do not import verl or claim validation of GPU/native trainer actors.
Run this file explicitly; it creates and removes only its own test actors.
"""

from concurrent.futures import ThreadPoolExecutor
import uuid

import pytest
import ray

from multi_task_scheduler.integration.verl.ray_actor import unwrap_native_actor_class
from multi_task_scheduler.scheduler import discovery
from multi_task_scheduler.scheduler.group_scheduler import RUNTIME_KIND


pytestmark = pytest.mark.ray_integration


@ray.remote(num_cpus=0)
class TaskRunnerProbe:
    """Test-only handle target; this is not the actual MultiTask TaskRunner."""

    def identity(self):
        return ray.get_runtime_context().get_actor_id()


@ray.remote(num_cpus=0)
class BaseProbe:
    """Test-only decorated parent; this is deliberately not a verl class."""

    def __init__(self, value):
        self.value = value

    def base_value(self):
        return self.value


@pytest.fixture
def isolated_ray(monkeypatch):
    assert not ray.is_initialized(), "Run the selected GS tests outside any existing Ray session"
    name = f"multitask-gs-test-{uuid.uuid4().hex}"
    namespace = f"multitask-test-{uuid.uuid4().hex}"
    monkeypatch.setattr(discovery, "GROUP_SCHEDULER_NAME", name)
    monkeypatch.setattr(discovery, "GROUP_SCHEDULER_NAMESPACE", namespace)
    ray.init(address="local", num_cpus=2, num_gpus=0, include_dashboard=False, _node_ip_address="127.0.0.1")
    try:
        yield
    finally:
        try:
            scheduler = ray.get_actor(name, namespace=namespace)
        except ValueError:
            pass
        else:
            ray.kill(scheduler, no_restart=True)
        ray.shutdown()


def test_discovery_requires_an_initialized_ray_runtime():
    assert not ray.is_initialized()
    with pytest.raises(RuntimeError):
        discovery.get_or_create_group_scheduler()


def test_concurrent_discovery_returns_one_real_scheduler_and_round_trips_handles(isolated_ray):
    with ThreadPoolExecutor(max_workers=4) as executor:
        schedulers = list(executor.map(lambda _: discovery.get_or_create_group_scheduler(), range(8)))
    assert all(isinstance(scheduler, ray.actor.ActorHandle) for scheduler in schedulers)
    assert len({scheduler._actor_id for scheduler in schedulers}) == 1
    scheduler = schedulers[0]
    assert ray.get(scheduler.runtime_kind.remote()) == RUNTIME_KIND
    assert ray.get(scheduler.schedule.remote()) == []

    first = TaskRunnerProbe.remote()
    second = TaskRunnerProbe.remote()
    try:
        ray.get(scheduler.attach_task.remote("task-a", first))
        ray.get(scheduler.attach_task.remote("task-a", first))
        with pytest.raises(ValueError):
            ray.get(scheduler.attach_task.remote("task-a", second))
        handles = ray.get(scheduler.get_task_runners.remote())
        assert set(handles) == {"task-a"}
        assert ray.get(handles["task-a"].identity.remote()) == ray.get(first.identity.remote())
        with pytest.raises((TypeError, ValueError)):
            ray.get(scheduler.attach_task.remote("task-b", object()))
        ray.get(scheduler.detach_task.remote("task-a"))
        ray.get(scheduler.detach_task.remote("task-a"))
        assert ray.get(scheduler.get_task_runners.remote()) == {}
    finally:
        ray.kill(first, no_restart=True)
        ray.kill(second, no_restart=True)


def test_real_ray_unwrap_subclass_and_remote_constructor_delegate_to_parent(isolated_ray):
    """Prove the Ray mechanism only, not actual verl parent initialization."""

    class ChildProbe(unwrap_native_actor_class(BaseProbe)):
        def __init__(self, value):
            super().__init__(value)
            self.child_initialized = True

        def describe(self):
            return super().base_value(), self.child_initialized, type(self).__name__

    child_actor_class = ray.remote(num_cpus=0)(ChildProbe)
    child = child_actor_class.remote(17)
    try:
        assert ray.get(child.base_value.remote()) == 17
        assert ray.get(child.describe.remote()) == (17, True, "ChildProbe")
    finally:
        ray.kill(child, no_restart=True)
