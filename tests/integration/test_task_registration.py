"""Real CPU Ray checks for registration and async metadata RPCs.

GPU selectors below are synthetic metadata, not Ray GPU allocations. These tests
do not import verl, launch real Trainer/Server actors or validate GPU placement.
"""

from dataclasses import FrozenInstanceError, replace
from types import SimpleNamespace

import pytest
import ray

from multi_task_scheduler.integration.verl.resource_query import collect_training_nodes, read_node_metadata
from multi_task_scheduler.integration.verl import resource_query
from multi_task_scheduler.scheduler import discovery
from multi_task_scheduler.scheduler.registration import (
    NativeReplicaResources,
    NodeInfo,
    RegistrationError,
    ReplicaNodeResources,
    TaskResourceRegistration,
)
from test_group_scheduler import TaskRunnerProbe, isolated_ray  # noqa: F401 — shared isolated test fixture


pytestmark = pytest.mark.ray_integration


@ray.remote(num_cpus=0)
class AsyncWorkerProbe:
    """A CPU Actor with an event loop, not a verl Worker or HTTP server."""

    async def ping(self):
        return "ready"


@ray.remote(num_cpus=0)
class AsyncCollectorProbe:
    """Run the real node collector inside an async CPU Actor."""

    async def collect(self, worker):
        group = SimpleNamespace(workers=[worker])
        return await collect_training_nodes({"actor": group, "ref": group})

    async def exercise_reader(self, worker, mode, timeout):
        # Exercise the real wait/error boundary without inventing a new production RPC.
        # Test-only callbacks are serialized by value; workers need not import pytest's module.
        def reader(actor):
            if mode == "fail":
                raise RuntimeError("intentional CPU reader failure")
            import time

            time.sleep(0.1)  # Test-only read latency, not a production retry delay.
            return read_node_metadata(actor)

        return await resource_query._bounded_gather(
            [resource_query._query_batch([(worker, reader, "CPU probe")])],
            label="CPU probe", timeout=timeout,
        )

@ray.remote(num_cpus=0)
class IncompatibleSchedulerProbe:
    """An incompatible named test Actor; never replace a real P1 scheduler."""

    def runtime_kind(self):
        return "verl-multi-task:experimental_fully_async_standalone:p1"


def _registration(task_id, gpu_ids=("synthetic-4", "synthetic-5")):
    node = NodeInfo(ray.get_runtime_context().get_node_id(), ray.util.get_node_ip_address())
    return TaskResourceRegistration(
        task_id, (node,), (node.node_id,),
        (NativeReplicaResources(0, (ReplicaNodeResources(0, node.node_id, gpu_ids),)),),
    )


def test_real_registration_serialization_idempotence_and_metadata_cleanup(isolated_ray):
    scheduler = discovery.get_or_create_group_scheduler()
    runner = TaskRunnerProbe.remote()
    try:
        task_id = ray.get(runner.identity.remote(), timeout=30)
        registration = _registration(task_id)
        ray.get(scheduler.attach_task.remote(task_id, runner), timeout=30)
        assert ray.get(scheduler.register_task_resources.remote(registration), timeout=30) == {
            "task_id": task_id, "status": "REGISTERED",
        }
        assert ray.get(scheduler.register_task_resources.remote(registration), timeout=30)["status"] == "ALREADY_REGISTERED"
        returned = ray.get(scheduler.get_task_resources.remote(task_id), timeout=30)
        assert returned == registration
        with pytest.raises(FrozenInstanceError):
            returned.task_id = "invalid"

        view = ray.get(scheduler.get_resource_view.remote(), timeout=30)
        assert set(view["tasks"]) == {task_id}
        node_view = view["nodes"][registration.nodes[0].node_id]
        assert node_view["training_task_ids"] == [task_id]
        assert set(node_view["gpu_owners"]) == {"synthetic-4", "synthetic-5"}
        node_view["gpu_owners"].clear()
        view["tasks"].clear()
        assert ray.get(scheduler.get_task_resources.remote(task_id), timeout=30) == registration
        assert ray.get(scheduler.schedule.remote(), timeout=30) == []

        changed = replace(registration, training_node_ids=())
        with pytest.raises(RegistrationError):
            ray.get(scheduler.register_task_resources.remote(changed), timeout=30)
        assert ray.get(scheduler.get_task_resources.remote(task_id), timeout=30) == registration

        ray.get(scheduler.detach_task.remote(task_id), timeout=30)
        ray.get(scheduler.detach_task.remote(task_id), timeout=30)
        assert ray.get(scheduler.get_task_resources.remote(task_id), timeout=30) is None
        assert ray.get(scheduler.get_resource_view.remote(), timeout=30) == {"tasks": {}, "nodes": {}}
        with pytest.raises(RegistrationError, match="attach"):
            ray.get(scheduler.register_task_resources.remote(registration), timeout=30)
    finally:
        ray.kill(runner, no_restart=True)


def test_concurrent_conflicting_registrations_commit_only_one_complete_owner(isolated_ray):
    scheduler = discovery.get_or_create_group_scheduler()
    runners = [TaskRunnerProbe.remote(), TaskRunnerProbe.remote()]
    try:
        task_ids = ray.get([runner.identity.remote() for runner in runners], timeout=30)
        ray.get([scheduler.attach_task.remote(task_id, runner)
                 for task_id, runner in zip(task_ids, runners, strict=True)], timeout=30)
        submissions = [scheduler.register_task_resources.remote(_registration(task_id)) for task_id in task_ids]
        successes, failures = [], []
        for submission in submissions:
            try:
                successes.append(ray.get(submission, timeout=30))
            except RegistrationError as error:
                failures.append(error)
        assert len(successes) == len(failures) == 1
        assert "GPU ownership conflict" in str(failures[0])
        winner = successes[0]["task_id"]
        loser = next(task_id for task_id in task_ids if task_id != winner)
        assert set(ray.get(scheduler.get_resource_view.remote(), timeout=30)["tasks"]) == {winner}
        assert ray.get(scheduler.get_task_resources.remote(loser), timeout=30) is None

        # Different GPUs on the same node are legal. Both training-node links remain.
        ray.get(scheduler.register_task_resources.remote(_registration(loser, ("synthetic-6",))), timeout=30)
        ray.get(scheduler.detach_task.remote(winner), timeout=30)
        remaining = ray.get(scheduler.get_resource_view.remote(), timeout=30)
        assert set(remaining["tasks"]) == {loser}
        assert len(remaining["nodes"]) == 1
    finally:
        for runner in runners:
            ray.kill(runner, no_restart=True)


def test_importable_node_reader_runs_on_real_async_actor_and_nested_collector(isolated_ray):
    worker = AsyncWorkerProbe.remote()
    collector = AsyncCollectorProbe.remote()
    try:
        assert ray.get(worker.ping.remote(), timeout=30) == "ready"
        actual = ray.get(worker.__ray_call__.remote(read_node_metadata), timeout=30)
        assert actual.node_id == ray.get_runtime_context().get_node_id()
        assert actual.node_ip
        assert ray.get(collector.collect.remote(worker), timeout=30) == (actual,)
        with pytest.raises(RegistrationError, match="intentional CPU reader failure"):
            ray.get(collector.exercise_reader.remote(worker, "fail", 5), timeout=30)
        with pytest.raises(RegistrationError, match="timed out"):
            ray.get(collector.exercise_reader.remote(worker, "slow", 0.01), timeout=30)
        # The callback and collector leave the worker event loop usable.
        assert ray.get(worker.ping.remote(), timeout=30) == "ready"
        assert ray.get(collector.collect.remote(worker), timeout=30) == (actual,)
    finally:
        ray.kill(collector, no_restart=True)
        ray.kill(worker, no_restart=True)


def test_discovery_rejects_an_old_contract_without_replacing_the_actor(isolated_ray):
    old = IncompatibleSchedulerProbe.options(
        name=discovery.GROUP_SCHEDULER_NAME,
        namespace=discovery.GROUP_SCHEDULER_NAMESPACE,
        lifetime="detached",
    ).remote()
    old_kind = ray.get(old.runtime_kind.remote(), timeout=30)
    with pytest.raises(RuntimeError, match="incompatible runtime"):
        discovery.get_or_create_group_scheduler()
    assert ray.get(old.runtime_kind.remote(), timeout=30) == old_kind
    assert ray.get_actor(discovery.GROUP_SCHEDULER_NAME, namespace=discovery.GROUP_SCHEDULER_NAMESPACE) == old
