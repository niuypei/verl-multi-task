# Copyright 2025 Meituan Ltd. and/or its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Replace creation targets while inheriting the native initialization and fit loop."""

import logging

import ray

from verl.experimental.fully_async_policy.fully_async_main import FullyAsyncTaskRunner
from verl.experimental.separation.utils import create_resource_pool_manager
from verl.trainer.ppo.utils import Role

from multi_task_scheduler.integration.verl.ray_actor import unwrap_native_actor_class
from multi_task_scheduler.scheduler.discovery import get_or_create_group_scheduler
from multi_task_scheduler.scheduler.registration import build_task_resource_registration

from .rollouter import MultiTaskFullyAsyncRollouter
from .trainer import MultiTaskFullyAsyncTrainer

logger = logging.getLogger(__name__)
REGISTRATION_TIMEOUT_S = 30


@ray.remote(num_cpus=1)
class MultiTaskFullyAsyncTaskRunner(unwrap_native_actor_class(FullyAsyncTaskRunner)):
    """Own GS/Trainer/Rollouter handles, but no rollout or CE Manager objects."""

    def __init__(self):
        super().__init__()
        self.group_scheduler = None

    def run(self, config):
        """Attach this Actor to GS, then execute verl's original run method."""
        self.group_scheduler = get_or_create_group_scheduler()
        context = ray.get_runtime_context()
        task_id = context.get_actor_id()
        try:
            ray.get(self.group_scheduler.attach_task.remote(task_id, context.current_actor), timeout=30)
            return super().run(config)
        finally:
            try:
                ray.get(self.group_scheduler.detach_task.remote(task_id), timeout=30)
            except Exception:
                # Cleanup must not replace the original initialization/training error.
                logger.warning("Could not detach TaskRunner %s from GroupScheduler", task_id, exc_info=True)

    def _initialize_components(self, config) -> None:
        """Register observed resources after native initialization, before either fit."""
        super()._initialize_components(config)
        if self.group_scheduler is None:
            raise RuntimeError("Resource registration requires the TaskRunner's GroupScheduler handle")

        training_nodes, rollout_metadata = ray.get(
            [
                self.components["trainer"].collect_training_nodes.remote(),
                self.components["rollouter"].collect_rollout_resources.remote(),
            ],
            timeout=REGISTRATION_TIMEOUT_S,
        )
        registration = build_task_resource_registration(
            task_id=ray.get_runtime_context().get_actor_id(),
            training_nodes=training_nodes,
            rollout_metadata=rollout_metadata,
        )
        # A timeout does not prove that GS rejected the first submission. Retry
        # exactly the same immutable payload once; never recollect a new topology.
        for attempt in range(2):
            try:
                result = ray.get(
                    self.group_scheduler.register_task_resources.remote(registration),
                    timeout=REGISTRATION_TIMEOUT_S,
                )
            except ray.exceptions.GetTimeoutError as error:
                if attempt == 1:
                    raise RuntimeError(
                        f"Could not confirm resource registration for task {registration.task_id}; "
                        "the GS commit outcome is unknown"
                    ) from error
                logger.warning("Retrying timed-out resource registration for task %s", registration.task_id)
                continue
            if (
                not isinstance(result, dict)
                or result.get("task_id") != registration.task_id
                or result.get("status") not in ("REGISTERED", "ALREADY_REGISTERED")
            ):
                raise RuntimeError(f"Invalid resource registration result for task {registration.task_id}")
            logger.info(
                "Registered task %s: %d training nodes, %d native rollout replicas",
                registration.task_id, len(registration.training_node_ids), len(registration.rollout_replicas),
            )
            return

    def _create_rollouter(self, config) -> None:
        """Preserve native main.py:117-136; replace only the type and GS argument."""
        print("[ASYNC MAIN] Starting create rollouter...")
        rollouter = MultiTaskFullyAsyncRollouter.remote(
            config=config,
            tokenizer=self.components["tokenizer"],
            processor=self.components["processor"],
            device_name=config.trainer.device,
            group_scheduler=self.group_scheduler,
        )

        if "hybrid_worker_group" in self.components:
            ray.get(rollouter.set_hybrid_worker_group.remote(self.components["hybrid_worker_group"]))
            print("[ASYNC MAIN] Hybrid worker group injected into rollouter")

        ray.get(rollouter.init_workers.remote())
        ray.get(rollouter.set_max_required_samples.remote())

        self.components["rollouter"] = rollouter
        print("[ASYNC MAIN] Rollouter created and initialized successfully")

    def _create_trainer(self, config) -> None:
        """Preserve native main.py:138-157; replace only the Trainer ActorClass."""
        print("[ASYNC MAIN] Starting create trainer...")
        trainer_role_mapping = {
            role: worker_cls
            for role, worker_cls in self.components["role_worker_mapping"].items()
            if role != Role.Rollout
        }

        trainer = MultiTaskFullyAsyncTrainer.remote(
            config=config,
            tokenizer=self.components["tokenizer"],
            role_worker_mapping=trainer_role_mapping,
            resource_pool_manager=create_resource_pool_manager(config, roles=list(trainer_role_mapping.keys())),
            ray_worker_group_cls=self.components["ray_worker_group_cls"],
            device_name=config.trainer.device,
        )

        ray.get(trainer.init_workers.remote())
        self.components["trainer"] = trainer
        print("[ASYNC MAIN] FullyAsyncTrainer created and initialized successfully")
