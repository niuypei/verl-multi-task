"""Read initial placement metadata without changing native runtime objects.

Ray's injected ``__ray_call__`` is the only private Actor interface used here.
The target-process readers are importable functions; Ray and verl device helpers
are imported only inside those readers. CPU metadata tests need neither verl nor
a GPU runtime. GPU selectors are not hardware UUIDs or evidence of free memory.
"""

import asyncio

from multi_task_scheduler.scheduler.registration import (
    NativeReplicaResources,
    NodeInfo,
    RegistrationError,
    ReplicaNodeResources,
    merge_node_infos,
)


QUERY_TIMEOUT_SECONDS = 30


def read_node_metadata(actor):
    """Execute inside a physical Worker Actor; do not read training GPU IDs."""
    import ray

    return NodeInfo(
        node_id=ray.get_runtime_context().get_node_id(),
        node_ip=ray.util.get_node_ip_address(),
    )


def read_rollout_worker_metadata(actor):
    """Return this CE Worker's node and full Ray-assigned selector list."""
    import ray
    from verl.utils.device import get_resource_name

    gpu_ids = ray.get_runtime_context().get_accelerator_ids().get(get_resource_name(), [])
    return read_node_metadata(actor), tuple(gpu_ids)


def read_server_metadata(actor):
    """Read the server's actual node, native ranks and visible-device order.

    Native HTTP servers do not request Ray GPUs. Their accelerator allocation
    cannot substitute for the CVD assigned by the replica at server creation.
    """
    import os

    import ray
    from verl.utils.device import get_visible_devices_keyword

    visible = os.environ.get(get_visible_devices_keyword(), "")
    return (
        ray.get_runtime_context().get_node_id(),
        actor.replica_rank,
        actor.node_rank,
        tuple(visible.split(",")) if visible else (),
    )


async def _read_actor(actor, reader, label):
    try:
        return await actor.__ray_call__.remote(reader)
    except Exception as exc:
        raise RegistrationError(f"{label}: metadata query failed: {exc}") from exc


async def _bounded_gather(coroutines, *, label, timeout):
    """Bound the whole batch and cancel only local waits, never Actor work."""
    pending = asyncio.gather(*coroutines)
    try:
        return await asyncio.wait_for(pending, timeout=timeout)
    except asyncio.TimeoutError as exc:
        raise RegistrationError(f"{label}: metadata query timed out after {timeout}s") from exc
    finally:
        # On another query's failure, gather does not cancel its other children.
        # The individual local tasks are managed by the caller's batch below.
        if not pending.done():
            pending.cancel()


async def _query_batch(queries):
    """Drain cancellation of local awaiters, including partial batch failure."""
    tasks = [asyncio.create_task(_read_actor(*query)) for query in queries]
    try:
        return await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def collect_training_nodes(all_wg, *, timeout=QUERY_TIMEOUT_SECONDS):
    """Read actor/critic/ref physical workers, retaining no runtime handles."""
    if "actor" not in all_wg:
        raise RegistrationError("Training metadata requires the native actor WorkerGroup")
    queries = []
    for role in ("actor", "critic", "ref"):
        if role not in all_wg:
            continue
        workers = tuple(all_wg[role].workers)
        if not workers:
            raise RegistrationError(f"Training {role} WorkerGroup is empty")
        queries.extend((worker, read_node_metadata, f"training {role} worker {rank}")
                       for rank, worker in enumerate(workers))
    results, = await _bounded_gather(
        [_query_batch(queries)], label="training nodes", timeout=timeout
    )
    return merge_node_infos(results)


def _checked_rank(value, label, *, positive=False):
    if type(value) is not int or value < (1 if positive else 0):
        raise RegistrationError(f"{label} must be a {'positive' if positive else 'nonnegative'} integer")
    return value


async def _collect_replica(replica):
    rank = _checked_rank(replica.replica_rank, "replica_rank")
    label = f"rollout replica {rank}"
    if getattr(replica.rollout_mode, "value", None) != "standalone":
        raise RegistrationError(f"{label} is not STANDALONE")
    if replica.is_reward_model or replica.is_teacher_model:
        raise RegistrationError(f"{label} is a reward/teacher replica, not main rollout")
    world_size = _checked_rank(replica.world_size, f"{label} world_size", positive=True)
    nnodes = _checked_rank(replica.nnodes, f"{label} nnodes", positive=True)
    block_size = _checked_rank(replica.gpus_per_replica_node, f"{label} block size", positive=True)
    workers, servers = tuple(replica.workers), tuple(replica.servers)
    if len(workers) != world_size or world_size != nnodes * block_size:
        raise RegistrationError(f"{label}: Worker count/world_size/node block sizes disagree")
    if len(servers) != nnodes:
        raise RegistrationError(f"{label}: Server count differs from nnodes")

    queries = [(worker, read_rollout_worker_metadata, f"{label} worker {i}")
               for i, worker in enumerate(workers)]
    queries.extend((server, read_server_metadata, f"{label} server {i}")
                   for i, server in enumerate(servers))
    results = await _query_batch(queries)
    worker_infos, server_infos = results[:world_size], results[world_size:]
    nodes = merge_node_infos(info[0] for info in worker_infos)
    for i, (_, gpu_ids) in enumerate(worker_infos):
        if (len(gpu_ids) != 1 or not isinstance(gpu_ids[0], str)
                or not gpu_ids[0].strip() or gpu_ids[0] != gpu_ids[0].strip() or "," in gpu_ids[0]):
            raise RegistrationError(f"{label} worker {i}: expected exactly one nonempty GPU selector")

    blocks, seen_nodes, seen_gpus = [], set(), set()
    for node_rank, server_info in enumerate(server_infos):
        block = worker_infos[node_rank * block_size:(node_rank + 1) * block_size]
        node_id = block[0][0].node_id
        if any(node.node_id != node_id for node, _ in block):
            raise RegistrationError(f"{label} node block {node_rank}: Workers span different nodes")
        if node_id in seen_nodes:
            raise RegistrationError(f"{label}: multiple node blocks occupy the same Ray node")
        seen_nodes.add(node_id)
        gpu_ids = tuple(gpus[0] for _, gpus in block)
        server_node, server_replica_rank, server_node_rank, server_gpu_ids = server_info
        _checked_rank(server_replica_rank, f"{label} server replica_rank")
        _checked_rank(server_node_rank, f"{label} server node_rank")
        if (server_node, server_replica_rank, server_node_rank) != (node_id, rank, node_rank):
            raise RegistrationError(f"{label} server {node_rank}: node/ranks disagree with Worker block")
        if server_gpu_ids != gpu_ids:
            raise RegistrationError(f"{label} server {node_rank}: visible GPU order differs from Workers")
        for gpu_id in gpu_ids:
            key = (node_id, gpu_id)
            if key in seen_gpus:
                raise RegistrationError(f"{label}: duplicate GPU ownership {key}")
            seen_gpus.add(key)
        blocks.append(ReplicaNodeResources(node_rank, node_id, gpu_ids))
    return nodes, NativeReplicaResources(rank, tuple(blocks))


async def collect_rollout_resources(replicas, *, timeout=QUERY_TIMEOUT_SECONDS):
    """Collect one complete native topology while preserving native rank order."""
    replicas = tuple(replicas)
    if not replicas:
        raise RegistrationError("Main STANDALONE rollout replicas are empty")
    tasks = [asyncio.create_task(_collect_replica(replica)) for replica in replicas]
    try:
        results = await _bounded_gather(tasks, label="rollout resources", timeout=timeout)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    nodes = merge_node_infos(node for replica_nodes, _ in results for node in replica_nodes)
    resources = tuple(resource for _, resource in results)
    ranks, gpu_keys = set(), set()
    for resource in resources:
        if resource.replica_rank in ranks:
            raise RegistrationError(f"Duplicate rollout replica_rank {resource.replica_rank}")
        ranks.add(resource.replica_rank)
        for block in resource.nodes:
            for gpu_id in block.gpu_ids:
                key = (block.node_id, gpu_id)
                if key in gpu_keys:
                    raise RegistrationError(f"Different native replicas claim the same GPU {key}")
                gpu_keys.add(key)
    return nodes, resources
