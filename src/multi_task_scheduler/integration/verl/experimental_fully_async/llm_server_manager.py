"""Select rollout subclasses without replacing native resource initialization."""

import ray

from verl.experimental.fully_async_policy.fully_async_rollouter import FullyAsyncLLMServerManager
from verl.workers.rollout.llm_server import DEFAULT_ROUTING_CACHE_SIZE

from multi_task_scheduler.integration.verl.resource_query import collect_rollout_resources
from multi_task_scheduler.rollout.load_balancer import MultiTaskGlobalRequestLoadBalancer
from multi_task_scheduler.rollout.replica import MultiTaskvLLMReplica


class MultiTaskLLMServerManager(FullyAsyncLLMServerManager):
    """Ordinary object owned by Rollouter; native replica lists remain authoritative."""

    def __init__(self, config, worker_group=None, rollout_resource_pool=None, *, group_scheduler=None):
        self.group_scheduler = group_scheduler
        # LLMServerManager explicitly preserves a preselected replica class.
        self.rollout_replica_class = MultiTaskvLLMReplica
        super().__init__(config, worker_group, rollout_resource_pool)
        self._load_balancer_cls = MultiTaskGlobalRequestLoadBalancer

    async def collect_rollout_resources(self):
        """Query owned native runtimes without changing placement, CE or routing."""
        return await collect_rollout_resources(self.get_standalone_replicas())

    async def _init_global_load_balancer(self) -> None:
        # Native code forwards full_determinism only to its exact default class.
        # Our subclass keeps native routing, so it must receive the same flag.
        self.global_load_balancer = ray.remote(self._load_balancer_cls).remote(
            servers=dict(zip(self.server_addresses, self.server_handles, strict=True)),
            max_cache_size=DEFAULT_ROUTING_CACHE_SIZE,
            full_determinism=getattr(self.rollout_config, "full_determinism", False),
            group_scheduler=self.group_scheduler,
        )
