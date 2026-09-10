"""Pure metadata tests and local logic tests of the real Ray-unwrapped GS class.

GS tests seed the existing handle map with explicit placeholders. They do not
exercise attach, create Actors, invoke RPCs, or validate GPU/verl initialization.
"""

from dataclasses import FrozenInstanceError, fields, replace

import pytest

from multi_task_scheduler.scheduler.registration import (
    NativeReplicaResources,
    NodeInfo,
    RegistrationError,
    ReplicaNodeResources,
    TaskResourceRegistration,
    build_task_resource_registration,
    derive_resource_view,
    merge_node_infos,
    normalize_registration,
)


def sample(task_id="task-a"):
    return TaskResourceRegistration(
        task_id=task_id,
        nodes=(NodeInfo("r1", "10.0.0.3"), NodeInfo("t0", None), NodeInfo("r0", "10.0.0.2")),
        training_node_ids=("t0", "r0"),
        rollout_replicas=(
            NativeReplicaResources(1, (ReplicaNodeResources(0, "r1", ("7", "3")),)),
            NativeReplicaResources(0, (ReplicaNodeResources(0, "r0", ("6", "4")),)),
        ),
    )


def with_block(**kwargs):
    registration = sample()
    replica = registration.rollout_replicas[0]
    changed = replace(replica, nodes=(replace(replica.nodes[0], **kwargs),))
    return replace(registration, rollout_replicas=(changed, registration.rollout_replicas[1]))


def test_record_fields_match_the_registration_contract():
    expected = {
        NodeInfo: ("node_id", "node_ip"),
        ReplicaNodeResources: ("node_rank", "node_id", "gpu_ids"),
        NativeReplicaResources: ("replica_rank", "nodes"),
        TaskResourceRegistration: ("task_id", "nodes", "training_node_ids", "rollout_replicas"),
    }
    for record_type, names in expected.items():
        assert tuple(field.name for field in fields(record_type)) == names
        assert record_type.__dataclass_params__.frozen


def test_normalize_sorts_unordered_collections_but_preserves_gpu_order():
    original = sample()
    result = normalize_registration(original)
    assert result is not original
    assert tuple(node.node_id for node in result.nodes) == ("r0", "r1", "t0")
    assert result.training_node_ids == ("r0", "t0")
    assert tuple(replica.replica_rank for replica in result.rollout_replicas) == (0, 1)
    assert result.rollout_replicas[0].nodes[0].gpu_ids == ("6", "4")
    assert result.rollout_replicas[1].nodes[0].gpu_ids == ("7", "3")
    assert original == sample()
    assert normalize_registration(result) == result


def test_multinode_replica_orders_node_ranks_not_node_names():
    original = sample()
    replica = NativeReplicaResources(4, (
        ReplicaNodeResources(1, "r0", ("6", "4")),
        ReplicaNodeResources(0, "r1", ("7", "3")),
    ))
    result = normalize_registration(replace(original, rollout_replicas=(replica,)))
    assert result.rollout_replicas[0].nodes == tuple(reversed(replica.nodes))


@pytest.mark.parametrize("bad,match", [
    (None, "TaskResourceRegistration"),
    ({"task_id": "task-a"}, "TaskResourceRegistration"),
    (replace(sample(), task_id=""), "task_id"),
    (replace(sample(), task_id=True), "task_id"),
    (replace(sample(), task_id=" task-a"), "task_id"),
    (replace(sample(), nodes=()), "nodes must not be empty"),
    (replace(sample(), nodes=list(sample().nodes)), "immutable tuple"),
    (replace(sample(), nodes=sample().nodes + (sample().nodes[0],)), "duplicate node IDs"),
    (replace(sample(), nodes=sample().nodes + (NodeInfo("unused", None),)), "unreferenced"),
    (replace(sample(), nodes=sample().nodes[:2]), "missing from directory"),
    (replace(sample(), nodes=(object(),)), "NodeInfo"),
    (replace(sample(), nodes=(NodeInfo("r0", object()),)), "node_ip"),
    (replace(sample(), nodes=(NodeInfo("", None),)), "node_id"),
    (replace(sample(), training_node_ids=()), "training_node_ids must not be empty"),
    (replace(sample(), training_node_ids=["t0", "r0"]), "immutable tuple"),
    (replace(sample(), training_node_ids=("t0", "t0")), "duplicate node IDs"),
    (replace(sample(), training_node_ids=("unknown",)), "missing from directory"),
    (replace(sample(), training_node_ids=(object(),)), "training_node_id"),
    (replace(sample(), rollout_replicas=()), "rollout_replicas must not be empty"),
    (replace(sample(), rollout_replicas=[sample().rollout_replicas[0]]), "immutable tuple"),
    (replace(sample(), rollout_replicas=(object(),)), "NativeReplicaResources"),
    (replace(sample(), rollout_replicas=(NativeReplicaResources(-1, ()),)), "replica_rank"),
    (replace(sample(), rollout_replicas=(NativeReplicaResources(True, ()),)), "replica_rank"),
    (replace(sample(), rollout_replicas=(NativeReplicaResources(1.0, ()),)), "replica_rank"),
    (replace(sample(), rollout_replicas=(NativeReplicaResources(0, ()),)), "replica.nodes must not be empty"),
    (replace(sample(), rollout_replicas=(NativeReplicaResources(0, (object(),)),)), "ReplicaNodeResources"),
    (replace(sample(), rollout_replicas=(sample().rollout_replicas[0],) * 2), "Duplicate replica_rank"),
    (with_block(node_rank=-1), "node_rank"),
    (with_block(node_rank=True), "node_rank"),
    (with_block(node_rank=1), "continuous from zero"),
    (with_block(node_id="absent"), "missing from directory"),
    (with_block(gpu_ids=()), "gpu_ids must not be empty"),
    (with_block(gpu_ids=["7", "3"]), "immutable tuple"),
    (with_block(gpu_ids=(7,)), "gpu_id"),
    (with_block(gpu_ids=(object(),)), "gpu_id"),
    (with_block(gpu_ids=(" ",)), "gpu_id"),
    (with_block(gpu_ids=("7", "7")), "Duplicate GPU ownership within task"),
])
def test_incomplete_mutable_or_runtime_bearing_payloads_are_rejected(bad, match):
    with pytest.raises(RegistrationError, match=match):
        normalize_registration(bad)


@pytest.mark.parametrize("nodes,match", [
    ((ReplicaNodeResources(0, "r0", ("1",)), ReplicaNodeResources(0, "r1", ("2",))), "Duplicate node_rank"),
    ((ReplicaNodeResources(0, "r0", ("1",)), ReplicaNodeResources(2, "r1", ("2",))), "continuous from zero"),
    ((ReplicaNodeResources(0, "r0", ("1",)), ReplicaNodeResources(1, "r0", ("2",))), "Repeated node ID"),
])
def test_replica_node_blocks_reject_duplicates_and_missing_ranks(nodes, match):
    with pytest.raises(RegistrationError, match=match):
        normalize_registration(replace(sample(), rollout_replicas=(NativeReplicaResources(0, nodes),)))


def test_two_replicas_cannot_claim_the_same_gpu_within_one_task():
    original = sample()
    extra = NativeReplicaResources(2, (ReplicaNodeResources(0, "r0", ("6",)),))
    with pytest.raises(RegistrationError, match="Duplicate GPU ownership within task"):
        normalize_registration(replace(original, rollout_replicas=(*original.rollout_replicas, extra)))


def test_subclasses_cannot_smuggle_extra_runtime_state():
    class AdditionalNode(NodeInfo):
        pass

    with pytest.raises(RegistrationError, match="NodeInfo"):
        merge_node_infos((AdditionalNode("r0", None),))


def test_merge_nodes_fills_missing_ips_and_deduplicates_observations():
    observations = (NodeInfo("n1", None), NodeInfo("n0", "10.0.0.1"), NodeInfo("n1", "10.0.0.2"))
    assert merge_node_infos(iter(observations)) == (NodeInfo("n0", "10.0.0.1"), NodeInfo("n1", "10.0.0.2"))
    assert merge_node_infos(reversed(observations)) == merge_node_infos(observations)
    assert merge_node_infos(()) == ()


@pytest.mark.parametrize("bad,match", [
    (None, "iterable"),
    ((object(),), "NodeInfo"),
    ((NodeInfo("n0", ""),), "node_ip"),
    ((NodeInfo("n0", "10.0.0.1"), NodeInfo("n0", "10.0.0.2")), "Conflicting node_ip"),
])
def test_merge_nodes_rejects_bad_metadata(bad, match):
    with pytest.raises(RegistrationError, match=match):
        merge_node_infos(bad)


def test_build_combines_training_and_rollout_metadata_without_duplicate_nodes():
    original = sample()
    training = (NodeInfo("t0", None), NodeInfo("r0", None), NodeInfo("r0", "10.0.0.2"))
    result = build_task_resource_registration(
        task_id="task-a", training_nodes=training,
        rollout_metadata=((NodeInfo("r1", "10.0.0.3"), NodeInfo("r0", "10.0.0.2")), original.rollout_replicas),
    )
    assert result == normalize_registration(original)


@pytest.mark.parametrize("rollout_metadata", [None, [], ({},), ({}, (), object())])
def test_build_requires_the_two_part_rollout_metadata_contract(rollout_metadata):
    with pytest.raises(RegistrationError, match="rollout_metadata"):
        build_task_resource_registration(task_id="a", training_nodes=(), rollout_metadata=rollout_metadata)


def test_build_rejects_empty_training_collection_and_cross_collector_ip_conflicts():
    original = sample()
    with pytest.raises(RegistrationError, match="training_node_ids must not be empty"):
        build_task_resource_registration(
            task_id="task-a", training_nodes=(), rollout_metadata=(original.nodes, original.rollout_replicas),
        )
    with pytest.raises(RegistrationError, match="Conflicting node_ip"):
        build_task_resource_registration(
            task_id="task-a", training_nodes=(NodeInfo("r0", "different-ip"),),
            rollout_metadata=(original.nodes, original.rollout_replicas),
        )


def test_view_keeps_training_and_rollout_associations_on_the_same_node():
    view = derive_resource_view({"task-a": sample()})
    assert view["nodes"]["r0"]["training_task_ids"] == ["task-a"]
    assert view["nodes"]["r0"]["rollout_owners"] == [
        {"task_id": "task-a", "replica_rank": 0, "node_rank": 0, "gpu_ids": ("6", "4")},
    ]
    assert view["nodes"]["r0"]["gpu_owners"]["4"]["task_id"] == "task-a"
    assert view["nodes"]["t0"]["gpu_owners"] == {}
    assert view["tasks"]["task-a"]["training_node_ids"] == ("r0", "t0")
    assert derive_resource_view({}) == {"tasks": {}, "nodes": {}}


def test_view_preserves_multiple_locator_observations_without_using_ips_as_keys():
    other = replace(sample("task-b"), rollout_replicas=(
        NativeReplicaResources(0, (ReplicaNodeResources(0, "r0", ("0",)),)),
        NativeReplicaResources(1, (ReplicaNodeResources(0, "r1", ("0",)),)),
    ), nodes=(NodeInfo("r1", "10.0.0.3"), NodeInfo("r0", "10.0.0.99"), NodeInfo("t0", None)))
    view = derive_resource_view({"task-b": other, "task-a": sample()})
    assert view["nodes"]["r0"]["node_ips"] == ["10.0.0.2", "10.0.0.99"]
    assert view["nodes"]["r0"]["training_task_ids"] == ["task-a", "task-b"]
    assert view["nodes"]["r0"]["gpu_owners"]["0"]["task_id"] == "task-b"


@pytest.mark.parametrize("resources,match", [
    ([], "mapping"),
    ({"wrong-id": sample()}, "key must match"),
    ({object(): sample()}, "task_resources key"),
    ({"task-a": sample(), "task-b": sample("task-b")}, "GPU ownership conflict"),
])
def test_view_validates_records_and_cross_task_conflicts(resources, match):
    with pytest.raises(RegistrationError, match=match):
        derive_resource_view(resources)


@pytest.fixture
def local_scheduler():
    # Real class implementation, local Python execution only. No Ray Actor is
    # created; attach/remote execution is deliberately outside this test layer.
    from multi_task_scheduler.scheduler.group_scheduler import GroupScheduler

    scheduler = GroupScheduler.__ray_actor_class__()
    scheduler.task_runners.update({"task-a": object(), "task-b": object()})
    return scheduler


def test_gs_registers_once_and_retries_canonical_equivalent_payload(local_scheduler):
    scheduler = local_scheduler
    assert scheduler.register_task_resources(sample()) == {"task_id": "task-a", "status": "REGISTERED"}
    saved = scheduler.task_resources["task-a"]
    assert scheduler.register_task_resources(normalize_registration(sample())) == {
        "task_id": "task-a", "status": "ALREADY_REGISTERED",
    }
    assert scheduler.task_resources == {"task-a": saved}
    assert scheduler.task_resources["task-a"] is saved
    assert scheduler.schedule() == []
    assert scheduler.runtime_kind().endswith(":registration-v1")


def test_gs_rejects_unattached_task_and_bad_messages_without_partial_writes(local_scheduler):
    scheduler = local_scheduler
    for invalid in (None, sample("not-attached"), with_block(gpu_ids=()), replace(sample(), training_node_ids=())):
        with pytest.raises(RegistrationError):
            scheduler.register_task_resources(invalid)
        assert scheduler.task_resources == {}
        assert set(scheduler.task_runners) == {"task-a", "task-b"}


def test_gs_rejects_topology_changes_and_other_task_conflicts_atomically(local_scheduler):
    scheduler = local_scheduler
    scheduler.register_task_resources(sample())
    before = scheduler.get_resource_view()
    for invalid, message in ((with_block(gpu_ids=("2",)), "Task topology conflict"),
                             (sample("task-b"), "GPU ownership conflict")):
        with pytest.raises(RegistrationError, match=message):
            scheduler.register_task_resources(invalid)
        assert scheduler.get_resource_view() == before
        assert set(scheduler.task_resources) == {"task-a"}


def test_gs_accepts_distinct_gpu_owners_on_shared_training_and_rollout_nodes(local_scheduler):
    scheduler = local_scheduler
    scheduler.register_task_resources(sample())
    other = replace(sample("task-b"), rollout_replicas=(
        NativeReplicaResources(0, (ReplicaNodeResources(0, "r0", ("0", "1")),)),
        NativeReplicaResources(1, (ReplicaNodeResources(0, "r1", ("0", "1")),)),
    ))
    assert scheduler.register_task_resources(other)["status"] == "REGISTERED"
    assert scheduler.get_resource_view()["nodes"]["r0"]["training_task_ids"] == ["task-a", "task-b"]
    scheduler.detach_task("task-a")
    assert scheduler.get_task_resources("task-b") == normalize_registration(other)
    assert scheduler.get_resource_view()["nodes"]["r0"]["training_task_ids"] == ["task-b"]


def test_gs_detach_removes_only_its_metadata_and_rejects_late_registration(local_scheduler):
    scheduler = local_scheduler
    scheduler.register_task_resources(sample())
    scheduler.detach_task("task-a")
    scheduler.detach_task("task-a")
    assert scheduler.task_resources == {}
    assert set(scheduler.task_runners) == {"task-b"}
    assert scheduler.get_task_resources("task-a") is None
    with pytest.raises(RegistrationError, match="attach"):
        scheduler.register_task_resources(sample())


def test_gs_returns_detached_immutable_records_and_fresh_mutable_views(local_scheduler):
    scheduler = local_scheduler
    scheduler.register_task_resources(sample())
    returned = scheduler.get_task_resources("task-a")
    saved = scheduler.task_resources["task-a"]
    assert returned is not saved
    assert returned.nodes[0] is not saved.nodes[0]
    assert returned.rollout_replicas[0].nodes[0] is not saved.rollout_replicas[0].nodes[0]
    with pytest.raises(FrozenInstanceError):
        returned.task_id = "changed"
    with pytest.raises(FrozenInstanceError):
        returned.rollout_replicas[0].nodes[0].gpu_ids = ("0",)
    view = scheduler.get_resource_view()
    view["nodes"]["r0"]["training_task_ids"].clear()
    view["nodes"]["r0"]["gpu_owners"]["4"]["task_id"] = "changed"
    view["tasks"]["task-a"]["nodes"][0]["node_id"] = "changed"
    assert scheduler.get_task_resources("task-a") == normalize_registration(sample())
    assert scheduler.get_resource_view() == derive_resource_view({"task-a": sample()})
