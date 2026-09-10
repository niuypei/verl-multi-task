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

"""Keep the native Trainer behavior and select the extended CE Manager."""

import ray

from verl.experimental.fully_async_policy.fully_async_trainer import FullyAsyncTrainer
from verl.utils.config import omega_conf_to_dataclass

from multi_task_scheduler.checkpoint.checkpoint_engine_manager import MultiTaskCheckpointEngineManager
from multi_task_scheduler.integration.verl.ray_actor import unwrap_native_actor_class
from multi_task_scheduler.integration.verl.resource_query import collect_training_nodes


@ray.remote(num_cpus=10)
class MultiTaskFullyAsyncTrainer(unwrap_native_actor_class(FullyAsyncTrainer)):
    """Own the CE Manager in this Actor; inherit training and weight synchronization."""

    async def collect_training_nodes(self):
        """Return only physical actor/critic/ref nodes after native initialization."""
        return await collect_training_nodes(self.all_wg)

    async def _setup_checkpoint_manager(self):
        """Preserve native trainer.py:217-224; replace only the Manager class."""
        replicas = await self.rollouter.get_replicas.remote()
        checkpoint_engine_config = omega_conf_to_dataclass(self.config.actor_rollout_ref.rollout.checkpoint_engine)
        self.checkpoint_manager = MultiTaskCheckpointEngineManager(
            config=checkpoint_engine_config, actor_wg=self.actor_wg, replicas=replicas
        )
        print(f"[FullyAsyncTrainer] Checkpoint manager initialized (backend={checkpoint_engine_config.backend})")
