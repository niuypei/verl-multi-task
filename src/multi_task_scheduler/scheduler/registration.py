"""Immutable startup resource metadata, without Ray or verl runtime imports.

These records describe observed native rollout ownership and training nodes.
They do not describe free GPUs or authorize resource sharing.
"""

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass


class RegistrationError(ValueError):
    """A startup registration is incomplete or conflicts with recorded metadata."""


@dataclass(frozen=True, slots=True)
class NodeInfo:
    node_id: str
    node_ip: str | None


@dataclass(frozen=True, slots=True)
class ReplicaNodeResources:
    node_rank: int
    node_id: str
    gpu_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class NativeReplicaResources:
    replica_rank: int
    nodes: tuple[ReplicaNodeResources, ...]


@dataclass(frozen=True, slots=True)
class TaskResourceRegistration:
    task_id: str
    nodes: tuple[NodeInfo, ...]
    training_node_ids: tuple[str, ...]
    rollout_replicas: tuple[NativeReplicaResources, ...]


def _identifier(value, field: str) -> str:
    if type(value) is not str or not value.strip() or value != value.strip():
        raise RegistrationError(f"{field} must be a nonempty string without surrounding whitespace")
    return value


def _tuple(value, field: str) -> tuple:
    if type(value) is not tuple:
        raise RegistrationError(f"{field} must be an immutable tuple")
    if not value:
        raise RegistrationError(f"{field} must not be empty")
    return value


def _rank(value, field: str) -> int:
    if type(value) is not int or value < 0:
        raise RegistrationError(f"{field} must be a nonnegative integer, not bool")
    return value


def _node_info(node) -> NodeInfo:
    if type(node) is not NodeInfo:
        raise RegistrationError("nodes must contain only NodeInfo records")
    node_id = _identifier(node.node_id, "node_id")
    node_ip = None if node.node_ip is None else _identifier(node.node_ip, "node_ip")
    return NodeInfo(node_id, node_ip)


def merge_node_infos(nodes: Iterable[NodeInfo]) -> tuple[NodeInfo, ...]:
    """Merge observations by node ID; fill missing IPs, but reject conflicting IPs."""
    try:
        iterator = iter(nodes)
    except TypeError as exc:
        raise RegistrationError("nodes must be an iterable of NodeInfo records") from exc
    merged = {}
    for item in iterator:
        node = _node_info(item)
        previous = merged.get(node.node_id)
        if previous is not None:
            if previous.node_ip is not None and node.node_ip is not None and previous.node_ip != node.node_ip:
                raise RegistrationError(f"Conflicting node_ip observations for node {node.node_id}")
            node = NodeInfo(node.node_id, previous.node_ip or node.node_ip)
        merged[node.node_id] = node
    return tuple(merged[node_id] for node_id in sorted(merged))


def normalize_registration(registration: TaskResourceRegistration) -> TaskResourceRegistration:
    """Validate the whole message, then return a detached, canonically ordered copy.

    Exact record/scalar types and tuple fields prevent runtime objects or mutable
    containers from entering the registry. GPU selector order remains unchanged.
    """
    if type(registration) is not TaskResourceRegistration:
        raise RegistrationError("registration must be a TaskResourceRegistration record")
    task_id = _identifier(registration.task_id, "task_id")
    nodes = tuple(_node_info(node) for node in _tuple(registration.nodes, "nodes"))
    node_ids = {node.node_id for node in nodes}
    if len(node_ids) != len(nodes):
        raise RegistrationError("Node directory contains duplicate node IDs")

    training_ids = tuple(
        _identifier(node_id, "training_node_id")
        for node_id in _tuple(registration.training_node_ids, "training_node_ids")
    )
    if len(set(training_ids)) != len(training_ids):
        raise RegistrationError("training_node_ids contains duplicate node IDs")

    references = set(training_ids)
    replica_ranks = set()
    gpu_keys = set()
    replicas = []
    for replica in _tuple(registration.rollout_replicas, "rollout_replicas"):
        if type(replica) is not NativeReplicaResources:
            raise RegistrationError("rollout_replicas must contain only NativeReplicaResources records")
        replica_rank = _rank(replica.replica_rank, "replica_rank")
        if replica_rank in replica_ranks:
            raise RegistrationError(f"Duplicate replica_rank {replica_rank}")
        replica_ranks.add(replica_rank)
        blocks = []
        block_ranks = set()
        block_node_ids = set()
        for block in _tuple(replica.nodes, "replica.nodes"):
            if type(block) is not ReplicaNodeResources:
                raise RegistrationError("replica.nodes must contain only ReplicaNodeResources records")
            node_rank = _rank(block.node_rank, "node_rank")
            node_id = _identifier(block.node_id, "replica node_id")
            if node_rank in block_ranks:
                raise RegistrationError(f"Duplicate node_rank {node_rank} in replica {replica_rank}")
            if node_id in block_node_ids:
                raise RegistrationError(f"Repeated node ID {node_id} in replica {replica_rank}")
            block_ranks.add(node_rank)
            block_node_ids.add(node_id)
            references.add(node_id)
            gpu_ids = tuple(_identifier(gpu_id, "gpu_id") for gpu_id in _tuple(block.gpu_ids, "gpu_ids"))
            for gpu_id in gpu_ids:
                key = (node_id, gpu_id)
                if key in gpu_keys:
                    raise RegistrationError(f"Duplicate GPU ownership within task {task_id}: {key}")
                gpu_keys.add(key)
            blocks.append(ReplicaNodeResources(node_rank, node_id, gpu_ids))
        if block_ranks != set(range(len(blocks))):
            raise RegistrationError(f"node_rank must be continuous from zero in replica {replica_rank}")
        replicas.append(NativeReplicaResources(replica_rank, tuple(sorted(blocks, key=lambda block: block.node_rank))))

    if references - node_ids:
        raise RegistrationError(f"Node references missing from directory: {sorted(references - node_ids)}")
    if node_ids - references:
        raise RegistrationError(f"Node directory contains unreferenced nodes: {sorted(node_ids - references)}")
    return TaskResourceRegistration(
        task_id=task_id,
        nodes=tuple(sorted(nodes, key=lambda node: node.node_id)),
        training_node_ids=tuple(sorted(training_ids)),
        rollout_replicas=tuple(sorted(replicas, key=lambda replica: replica.replica_rank)),
    )


def build_task_resource_registration(*, task_id, training_nodes, rollout_metadata) -> TaskResourceRegistration:
    """Combine the two collectors' metadata without accepting runtime handles."""
    if type(rollout_metadata) is not tuple or len(rollout_metadata) != 2:
        raise RegistrationError("rollout_metadata must be the pair (nodes, rollout_replicas)")
    training_nodes = merge_node_infos(training_nodes)
    rollout_nodes, rollout_replicas = rollout_metadata
    nodes = merge_node_infos((*training_nodes, *merge_node_infos(rollout_nodes)))
    return normalize_registration(TaskResourceRegistration(
        task_id=task_id,
        nodes=nodes,
        training_node_ids=tuple(node.node_id for node in training_nodes),
        rollout_replicas=rollout_replicas,
    ))


def derive_resource_view(task_resources: Mapping[str, TaskResourceRegistration]) -> dict:
    """Build fresh task/node dictionaries and reject conflicting native GPU owners.

    Multiple task observations may contain different locator IPs for a node.
    Keep those observations as ``node_ips``; only node IDs identify resources.
    No returned field indicates availability, physical release, or a lease.
    """
    if not isinstance(task_resources, Mapping):
        raise RegistrationError("task_resources must be a mapping of task IDs to registrations")
    registrations = []
    for task_id, value in task_resources.items():
        _identifier(task_id, "task_resources key")
        registration = normalize_registration(value)
        if task_id != registration.task_id:
            raise RegistrationError("task_resources key must match registration.task_id")
        registrations.append(registration)

    tasks = {}
    nodes = {}
    for registration in sorted(registrations, key=lambda item: item.task_id):
        task_id = registration.task_id
        tasks[task_id] = asdict(registration)
        for node in registration.nodes:
            entry = nodes.setdefault(node.node_id, {
                "node_id": node.node_id, "node_ips": [], "training_task_ids": [],
                "rollout_owners": [], "gpu_owners": {},
            })
            if node.node_ip is not None and node.node_ip not in entry["node_ips"]:
                entry["node_ips"].append(node.node_ip)
        for node_id in registration.training_node_ids:
            nodes[node_id]["training_task_ids"].append(task_id)
        for replica in registration.rollout_replicas:
            for block in replica.nodes:
                entry = nodes[block.node_id]
                owner = {"task_id": task_id, "replica_rank": replica.replica_rank, "node_rank": block.node_rank}
                entry["rollout_owners"].append({**owner, "gpu_ids": block.gpu_ids})
                for gpu_id in block.gpu_ids:
                    previous = entry["gpu_owners"].get(gpu_id)
                    if previous is not None:
                        raise RegistrationError(
                            f"GPU ownership conflict for {(block.node_id, gpu_id)}: "
                            f"task {previous['task_id']} and task {task_id}"
                        )
                    entry["gpu_owners"][gpu_id] = dict(owner)
    for entry in nodes.values():
        entry["node_ips"].sort()
    return {"tasks": tasks, "nodes": {node_id: nodes[node_id] for node_id in sorted(nodes)}}
