"""Execute real TaskRunner override bodies with explicit parent/RPC substitutes.

These tests prove startup ordering and fail-closed registration, not native verl
initialization, real Ray delivery, weight synchronization or GPU training.
"""

import logging
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from multi_task_scheduler.scheduler.registration import (
    NativeReplicaResources,
    NodeInfo,
    RegistrationError,
    ReplicaNodeResources,
    build_task_resource_registration,
)
from test_wiring import INTEGRATION, _isolated_class


class FakeGetTimeoutError(Exception):
    """Explicit substitute for the driver's Ray get deadline exception."""


def _runner(*, native_error=None, collect_error=None, outcomes=None, training_nodes=None, detach_error=None):
    events = []
    if training_nodes is None:
        training_nodes = (NodeInfo("train-node", "10.0.0.1"),)
    rollout = (
        (NodeInfo("rollout-node", "10.0.0.2"),),
        (
            NativeReplicaResources(0, (ReplicaNodeResources(0, "rollout-node", ("4", "5")),)),
            NativeReplicaResources(1, (ReplicaNodeResources(0, "rollout-node", ("6", "7")),)),
        ),
    )
    pending_outcomes = iter(outcomes or [{"task_id": "task-a", "status": "REGISTERED"}])

    def rpc(event, value=None):
        def invoke(*args):
            events.append(event)
            return value
        return SimpleNamespace(remote=Mock(side_effect=invoke))

    trainer = SimpleNamespace(collect_training_nodes=rpc("collect-training", "training-ref"))
    rollouter = SimpleNamespace(collect_rollout_resources=rpc("collect-rollout", "rollout-ref"))
    scheduler = SimpleNamespace(
        attach_task=rpc("attach"),
        register_task_resources=rpc("register", "registration-ref"),
        detach_task=rpc("detach"),
    )
    if detach_error:
        scheduler.detach_task.remote.side_effect = detach_error

    class Parent:
        def __init__(self):
            self.components = {}

        def run(self, config):
            self._initialize_components(config)
            self._run_training_loop()

        def _initialize_components(self, config):
            events.append("native-initialize")
            if native_error:
                raise native_error
            self.components.update(trainer=trainer, rollouter=rollouter)
            events.extend(("native-initial-sync", "native-initial-validation"))

        def _run_training_loop(self):
            events.append("native-fit")

    def get(reference, *, timeout):
        assert timeout == 30
        if isinstance(reference, list):
            assert reference == ["training-ref", "rollout-ref"]
            if collect_error:
                raise collect_error
            return training_nodes, rollout
        if reference == "registration-ref":
            outcome = next(pending_outcomes)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        return reference

    cls = _isolated_class(
        f"{INTEGRATION}/task_runner.py", "MultiTaskFullyAsyncTaskRunner", Parent,
        ray=SimpleNamespace(
            get=get,
            get_runtime_context=lambda: SimpleNamespace(get_actor_id=lambda: "task-a", current_actor=object()),
            exceptions=SimpleNamespace(GetTimeoutError=FakeGetTimeoutError),
        ),
        get_or_create_group_scheduler=lambda: scheduler,
        build_task_resource_registration=build_task_resource_registration,
        REGISTRATION_TIMEOUT_S=30,
        logger=logging.getLogger(__name__),
    )
    return cls(), scheduler, trainer, rollouter, events


def test_registration_runs_after_native_initialization_and_before_fit():
    runner, scheduler, trainer, rollouter, events = _runner()
    runner.run(object())
    assert events == [
        "attach", "native-initialize", "native-initial-sync", "native-initial-validation",
        "collect-training", "collect-rollout", "register", "native-fit", "detach",
    ]
    registration = scheduler.register_task_resources.remote.call_args.args[0]
    assert registration.task_id == "task-a"
    assert registration.training_node_ids == ("train-node",)
    assert len(registration.rollout_replicas) == 2
    assert registration.rollout_replicas[0].nodes[0].gpu_ids == ("4", "5")
    trainer.collect_training_nodes.remote.assert_called_once_with()
    rollouter.collect_rollout_resources.remote.assert_called_once_with()


def test_native_failure_never_collects_or_registers():
    failure = RuntimeError("native initialization failed")
    runner, scheduler, trainer, rollouter, events = _runner(native_error=failure)
    with pytest.raises(RuntimeError) as caught:
        runner.run(object())
    assert caught.value is failure
    assert events == ["attach", "native-initialize", "detach"]
    trainer.collect_training_nodes.remote.assert_not_called()
    rollouter.collect_rollout_resources.remote.assert_not_called()
    scheduler.register_task_resources.remote.assert_not_called()


@pytest.mark.parametrize("failure", [RuntimeError("Worker query failed"), FakeGetTimeoutError("collection timeout")])
def test_collection_failure_never_registers_or_enters_fit(failure):
    runner, scheduler, _, _, events = _runner(collect_error=failure)
    with pytest.raises(type(failure)) as caught:
        runner.run(object())
    assert caught.value is failure
    scheduler.register_task_resources.remote.assert_not_called()
    assert "native-fit" not in events
    assert events[-1] == "detach"


def test_incomplete_metadata_never_reaches_gs():
    runner, scheduler, _, _, events = _runner(training_nodes=())
    with pytest.raises(RegistrationError):
        runner.run(object())
    scheduler.register_task_resources.remote.assert_not_called()
    assert "native-fit" not in events
    assert events[-1] == "detach"


def test_only_timeout_retries_and_uses_the_identical_immutable_payload():
    runner, scheduler, trainer, rollouter, events = _runner(outcomes=[
        FakeGetTimeoutError("response lost after commit"),
        {"task_id": "task-a", "status": "ALREADY_REGISTERED"},
    ])
    runner.run(object())
    calls = scheduler.register_task_resources.remote.call_args_list
    assert len(calls) == 2
    assert calls[0].args[0] is calls[1].args[0]
    trainer.collect_training_nodes.remote.assert_called_once_with()
    rollouter.collect_rollout_resources.remote.assert_called_once_with()
    assert events[-3:] == ["register", "native-fit", "detach"]


def test_two_timeouts_stop_initialization_without_claiming_no_gs_commit():
    runner, scheduler, _, _, events = _runner(outcomes=[FakeGetTimeoutError(), FakeGetTimeoutError()])
    with pytest.raises(RuntimeError, match="commit outcome is unknown") as caught:
        runner.run(object())
    assert isinstance(caught.value.__cause__, FakeGetTimeoutError)
    assert scheduler.register_task_resources.remote.call_count == 2
    assert "native-fit" not in events
    assert events[-1] == "detach"


def test_registration_conflict_is_not_retried_and_cleanup_preserves_original_error():
    failure = RegistrationError("native GPU owner conflict")
    runner, scheduler, _, _, events = _runner(outcomes=[failure], detach_error=RuntimeError("GS unavailable"))
    with pytest.raises(RegistrationError) as caught:
        runner.run(object())
    assert caught.value is failure
    scheduler.register_task_resources.remote.assert_called_once()
    scheduler.detach_task.remote.assert_called_once_with("task-a")
    assert "native-fit" not in events


@pytest.mark.parametrize("result", [
    None, {}, {"task_id": "another-task", "status": "REGISTERED"},
    {"task_id": "task-a", "status": "PENDING"}, "REGISTERED",
])
def test_wrong_registration_response_never_enters_fit(result):
    runner, scheduler, _, _, events = _runner(outcomes=[result])
    with pytest.raises(RuntimeError, match="Invalid resource registration result"):
        runner.run(object())
    scheduler.register_task_resources.remote.assert_called_once()
    assert "native-fit" not in events


def test_direct_initialization_requires_an_attached_scheduler():
    runner, scheduler, trainer, _, events = _runner()
    with pytest.raises(RuntimeError, match="GroupScheduler handle"):
        runner._initialize_components(object())
    trainer.collect_training_nodes.remote.assert_not_called()
    scheduler.register_task_resources.remote.assert_not_called()
    assert "native-fit" not in events
