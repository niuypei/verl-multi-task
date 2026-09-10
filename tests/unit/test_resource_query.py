"""Registration collectors with explicit fake Actor/Ray/backend boundaries.

No verl parent classes, GPU engines or real Ray Actors run in this file. Fake
``__ray_call__`` returns awaitables with known target-process metadata. Reader
tests substitute Ray runtime context and the lazily imported device helpers.
"""

import asyncio
import subprocess
import sys
from types import SimpleNamespace

import pytest

from multi_task_scheduler.integration.verl import resource_query as query
from multi_task_scheduler.scheduler.registration import NodeInfo, RegistrationError


class FakeActor:
    def __init__(self, payload, *, error=None, hang=False):
        self.payload = payload
        self.error = error
        self.hang = hang
        self.readers = []
        self.finished_wait = False
        self.__ray_call__ = SimpleNamespace(remote=self._remote)

    async def _remote(self, reader):
        self.readers.append(reader.__name__)
        try:
            if self.error:
                raise self.error
            if self.hang:
                await asyncio.Future()
            return self.payload
        finally:
            self.finished_wait = True


def replica(rank=0, blocks=(("node-a", ("4", "5", "6", "7")),)):
    workers, servers = [], []
    for node_rank, (node_id, gpu_ids) in enumerate(blocks):
        workers.extend(FakeActor((NodeInfo(node_id, f"ip-{node_id}"), (gpu_id,))) for gpu_id in gpu_ids)
        servers.append(FakeActor((node_id, rank, node_rank, tuple(gpu_ids))))
    return SimpleNamespace(
        replica_rank=rank, rollout_mode=SimpleNamespace(value="standalone"),
        is_reward_model=False, is_teacher_model=False,
        world_size=len(workers), nnodes=len(blocks), gpus_per_replica_node=len(blocks[0][1]),
        workers=workers, servers=servers,
    )


def collect(*replicas):
    return asyncio.run(query.collect_rollout_resources(replicas))


def test_query_module_import_is_dependency_light():
    result = subprocess.run(
        [sys.executable, "-c", "import sys; import multi_task_scheduler.integration.verl.resource_query; "
         "assert 'ray' not in sys.modules; assert 'verl' not in sys.modules; assert 'torch' not in sys.modules"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr


def test_training_queries_actual_role_workers_deduplicates_nodes_and_ignores_other_roles():
    shared = FakeActor(NodeInfo("node-a", "ip-a"))
    critic = FakeActor(NodeInfo("node-b", "ip-b"))
    excluded = FakeActor(None, error=AssertionError("must not query reward/teacher/controller"))
    all_wg = {
        "actor": SimpleNamespace(workers=[shared]),
        "ref": SimpleNamespace(workers=[shared]),
        "critic": SimpleNamespace(workers=[critic]),
        "rm": SimpleNamespace(workers=[excluded]),
        "teacher": SimpleNamespace(workers=[excluded]),
    }
    result = asyncio.run(query.collect_training_nodes(all_wg))
    assert result == (NodeInfo("node-a", "ip-a"), NodeInfo("node-b", "ip-b"))
    assert shared.readers == ["read_node_metadata", "read_node_metadata"]
    assert critic.readers == ["read_node_metadata"]
    assert excluded.readers == []


@pytest.mark.parametrize("all_wg", [{}, {"actor": SimpleNamespace(workers=[])}])
def test_training_requires_nonempty_actor_group(all_wg):
    with pytest.raises(RegistrationError, match="actor|empty"):
        asyncio.run(query.collect_training_nodes(all_wg))


def test_training_node_ip_disagreement_is_not_hidden():
    all_wg = {"actor": SimpleNamespace(workers=[
        FakeActor(NodeInfo("node-a", "ip-a")), FakeActor(NodeInfo("node-a", "ip-other")),
    ])}
    with pytest.raises(RegistrationError):
        asyncio.run(query.collect_training_nodes(all_wg))


def test_four_replicas_on_two_nodes_keep_actual_gpu_order_and_native_runtime_lists():
    runtimes = [
        replica(0), replica(1, (("node-b", ("0", "1", "2", "3")),)),
        replica(2, (("node-a", ("0", "1", "2", "3")),)),
        replica(3, (("node-b", ("7", "5", "6", "4")),)),
    ]
    original_workers = [tuple(runtime.workers) for runtime in runtimes]
    original_servers = [tuple(runtime.servers) for runtime in runtimes]
    nodes, resources = collect(*runtimes)
    assert [node.node_id for node in nodes] == ["node-a", "node-b"]
    assert len(resources) == 4
    assert resources[0].nodes[0].gpu_ids == ("4", "5", "6", "7")
    assert resources[3].nodes[0].gpu_ids == ("7", "5", "6", "4")
    assert [tuple(runtime.workers) for runtime in runtimes] == original_workers
    assert [tuple(runtime.servers) for runtime in runtimes] == original_servers
    assert all(worker.readers == ["read_rollout_worker_metadata"]
               for runtime in runtimes for worker in runtime.workers)
    assert all(server.readers == ["read_server_metadata"]
               for runtime in runtimes for server in runtime.servers)


def test_multi_node_replica_preserves_both_node_blocks_not_only_head():
    _, resources = collect(replica(blocks=(("node-b", ("6", "7")), ("node-a", ("4", "5")))))
    blocks = resources[0].nodes
    assert [(block.node_rank, block.node_id, block.gpu_ids) for block in blocks] == [
        (0, "node-b", ("6", "7")), (1, "node-a", ("4", "5")),
    ]


@pytest.mark.parametrize("gpu_ids", [(), ("0", "1"), ("",), (0,), (" 0",), ("0,1",)])
def test_worker_requires_exactly_one_nonempty_string_selector(gpu_ids):
    runtime = replica()
    runtime.workers[0].payload = (NodeInfo("node-a", "ip-node-a"), gpu_ids)
    with pytest.raises(RegistrationError, match="one nonempty GPU selector"):
        collect(runtime)


@pytest.mark.parametrize("field,value,match", [
    ("rollout_mode", SimpleNamespace(value="hybrid"), "STANDALONE"),
    ("is_reward_model", True, "reward/teacher"),
    ("is_teacher_model", True, "reward/teacher"),
    ("world_size", 3, "Worker count"),
    ("gpus_per_replica_node", 2, "Worker count"),
    ("nnodes", 2, "Worker count"),
    ("replica_rank", True, "nonnegative integer"),
    ("world_size", 0, "positive integer"),
    ("workers", [], "Worker count"),
    ("servers", [], "Server count"),
])
def test_native_local_contract_fails_before_queries(field, value, match):
    runtime = replica()
    original_workers = tuple(runtime.workers)
    setattr(runtime, field, value)
    with pytest.raises(RegistrationError, match=match):
        collect(runtime)
    assert all(not worker.readers for worker in original_workers)


def test_empty_native_rollout_is_not_successful_metadata():
    with pytest.raises(RegistrationError, match="empty"):
        collect()


def test_split_worker_block_is_rejected_not_regrouped():
    runtime = replica()
    runtime.workers[1].payload = (NodeInfo("node-b", "ip-node-b"), ("5",))
    with pytest.raises(RegistrationError, match="span different nodes"):
        collect(runtime)


def test_same_ray_node_for_multiple_server_blocks_is_rejected():
    runtime = replica(blocks=(("node-a", ("0", "1")), ("node-a", ("2", "3"))))
    with pytest.raises(RegistrationError, match="same Ray node"):
        collect(runtime)


@pytest.mark.parametrize("payload,match", [
    (("wrong-node", 0, 0, ("4", "5", "6", "7")), "node/ranks disagree"),
    (("node-a", 1, 0, ("4", "5", "6", "7")), "node/ranks disagree"),
    (("node-a", 0, 1, ("4", "5", "6", "7")), "node/ranks disagree"),
    (("node-a", 0, 0, ("5", "4", "6", "7")), "visible GPU order"),
    (("node-a", 0, 0, ()), "visible GPU order"),
    (("node-a", False, 0, ("4", "5", "6", "7")), "nonnegative integer"),
])
def test_server_binding_must_match_worker_block(payload, match):
    runtime = replica()
    runtime.servers[0].payload = payload
    with pytest.raises(RegistrationError, match=match):
        collect(runtime)


def test_duplicate_gpu_within_replica_is_rejected():
    with pytest.raises(RegistrationError, match="duplicate GPU ownership"):
        collect(replica(blocks=(("node-a", ("4", "4")),)))


def test_duplicate_gpu_across_replicas_is_rejected():
    with pytest.raises(RegistrationError, match="same GPU"):
        collect(replica(0), replica(1))


def test_duplicate_replica_rank_is_rejected():
    with pytest.raises(RegistrationError, match="Duplicate rollout replica_rank"):
        collect(replica(0), replica(0, (("node-b", ("4", "5", "6", "7")),)))


@pytest.mark.parametrize("target", ["worker", "server"])
def test_query_failure_is_labelled_and_no_partial_result_is_returned(target):
    runtime = replica()
    actor = runtime.workers[0] if target == "worker" else runtime.servers[0]
    actor.error = RuntimeError("target failed")
    with pytest.raises(RegistrationError, match=f"replica 0 {target} 0.*target failed"):
        collect(runtime)


@pytest.mark.parametrize("kind", ["training", "rollout"])
def test_async_timeout_drains_local_waits_without_blocking_event_loop(kind):
    async def run():
        actor = FakeActor(NodeInfo("node-a", "ip-a"), hang=True)
        if kind == "training":
            work = query.collect_training_nodes({"actor": SimpleNamespace(workers=[actor])}, timeout=0.01)
        else:
            runtime = replica()
            runtime.workers[0] = actor
            work = query.collect_rollout_resources([runtime], timeout=0.01)
        task = asyncio.create_task(work)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not task.done()  # The loop remains runnable while the reader waits.
        with pytest.raises(RegistrationError, match="timed out"):
            await task
        assert actor.finished_wait
    asyncio.run(run())


def test_early_failure_cleans_other_replica_local_waits():
    async def run():
        failing = replica(0)
        failing.workers[0].error = RuntimeError("one replica failed")
        other = replica(1, (("node-b", ("4", "5", "6", "7")),))
        other.workers[0].hang = True
        with pytest.raises(RegistrationError, match="one replica failed"):
            await query.collect_rollout_resources([failing, other])
        assert other.workers[0].finished_wait
        assert asyncio.all_tasks() == {asyncio.current_task()}
    asyncio.run(run())


def test_external_cancellation_cleans_local_query_waits():
    async def run():
        runtime = replica()
        runtime.workers[0].hang = True
        task = asyncio.create_task(query.collect_rollout_resources([runtime]))
        for _ in range(10):
            if runtime.workers[0].readers:
                break
            await asyncio.sleep(0)
        assert runtime.workers[0].readers
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert runtime.workers[0].finished_wait
        assert asyncio.all_tasks() == {asyncio.current_task()}
    asyncio.run(run())


def install_reader_runtime(monkeypatch, *, gpu_ids=None):
    context = SimpleNamespace(get_node_id=lambda: "actual-worker-node")
    def get_accelerator_ids():
        if gpu_ids is None:
            raise AssertionError("This reader must not query GPU allocation")
        return {"GPU": gpu_ids}
    context.get_accelerator_ids = get_accelerator_ids
    monkeypatch.setitem(sys.modules, "ray", SimpleNamespace(
        get_runtime_context=lambda: context,
        util=SimpleNamespace(get_node_ip_address=lambda: "actual-worker-ip"),
    ))
    monkeypatch.setitem(sys.modules, "verl.utils.device", SimpleNamespace(
        get_resource_name=lambda: "GPU", get_visible_devices_keyword=lambda: "CUDA_VISIBLE_DEVICES",
    ))


def test_training_reader_reads_target_node_only(monkeypatch):
    install_reader_runtime(monkeypatch)
    assert query.read_node_metadata(object()) == NodeInfo("actual-worker-node", "actual-worker-ip")


def test_worker_reader_keeps_full_selector_list_without_integer_or_uuid_conversion(monkeypatch):
    install_reader_runtime(monkeypatch, gpu_ids=["7", "GPU-selector"])
    node, gpu_ids = query.read_rollout_worker_metadata(object())
    assert node.node_id == "actual-worker-node"
    assert gpu_ids == ("7", "GPU-selector")


def test_server_reader_reads_environment_not_gpu_allocation_or_nonexistent_member(monkeypatch):
    install_reader_runtime(monkeypatch)
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "7,4,6,5")
    server = SimpleNamespace(replica_rank=3, node_rank=1)
    assert query.read_server_metadata(server) == ("actual-worker-node", 3, 1, ("7", "4", "6", "5"))
