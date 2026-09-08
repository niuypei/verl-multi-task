# verl-multi-task

状态：已接通原生入口的类型选择、GS 发现和 MultiTask 创建链；新增调度业务为空。完整 verl/vLLM 运行待用户真实环境验证。

本仓是独立的 Python 源码库。用户仍从
`verl.experimental.fully_async_policy.fully_async_main` 启动，不使用另一个伴生训练入口。

## 当前代码做什么

- 原生入口根据唯一的 `multitask.runtime.profile` 选择真实 MultiTask TaskRunner ActorClass。
- TaskRunner 创建或获取全局 GS Ray Actor，GS 和 TaskRunner 互相持有句柄。
- TaskRunner、Trainer、Rollouter、LLM Manager、LB、Replica、HTTP Server 和 CE 组件沿原生创建链初始化。
- 扩展类复用原生 rollout、训练、MessageQueue、路由和参数同步；GS 的调度方法返回空列表。
- 当前没有租约、心跳循环、借卡、bootstrap、同步 gate 或另一套 replica registry。

当前适配的是 experimental Fully Async + 纯 STANDALONE + vLLM 非 PD 路径，不是 V1。
PD 指 Prefill/Decode 分离；该路径需要不同 Replica 类型，当前 profile 会明确拒绝，不静默回退。
未启用 profile 时，verl 不导入本包，原生行为保持不变。

## 源码部署与启动

本地开发源是 `verl-multi-task/src/multi_task_scheduler/`。
用户在真实环境将本仓源码放进 verl 仓根下的 `verl-multi-task/`，但不复制子仓 Git 元数据和虚拟环境。
源码嵌套不会自动加入 Python 搜索路径；所有 Ray 节点必须能读取相同版本源码。

以下命令从真实环境的 verl 仓根执行。用户先准备原生训练依赖、模型、数据与集群配置：

```bash
export PYTHONPATH="$PWD/verl-multi-task/src:$PWD"
python -m verl.experimental.fully_async_policy.fully_async_main \
  multitask.runtime.profile=experimental_fully_async_standalone \
  actor_rollout_ref.hybrid_engine=false \
  actor_rollout_ref.rollout.name=vllm \
  actor_rollout_ref.rollout.mode=async \
  actor_rollout_ref.rollout.checkpoint_engine.backend=nccl \
  actor_rollout_ref.rollout.calculate_log_probs=true \
  async_training.use_trainer_do_validate=false \
  async_training.use_dynamic_resource_scheduling=false \
  data.train_batch_size=0 data.gen_batch_size=1 \
  <原生模型、数据、trainer 和 rollout 资源参数>
```

最后一行是占位说明，用户必须替换为自己的原生参数。配置示例见 [examples](examples/experimental_fully_async/README.md)。
部署必须同时包含配套 verl 两处接线补丁；仅复制本仓不能让未修改的入口读取 profile。
`multitask.runtime.profile=null` 或缺失 profile 保持原生路径。非法配置、缺失依赖或不支持的类型会报错。

## 验证

子仓只默认收集 `tests/unit/`。操作者可以用项目虚拟环境安装 `requirements/ut.txt`，其中只有 pytest、PyYAML、Hydra 和 Ray。
这些依赖不包含完整 verl/vLLM 运行环境。

从本仓根执行：

```bash
MT_VERL_SOURCE_ROOT=/absolute/verl \
PYTHONPATH="$PWD/src" PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
.venv/bin/python -m pytest -q -p no:cacheprovider tests/unit

PYTHONPATH="$PWD/src" PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 RAY_USAGE_STATS_ENABLED=0 \
.venv/bin/python -m pytest -q -p no:cacheprovider tests/integration/test_group_scheduler.py
```

- unit：配置使用真实 OmegaConf；创建方法测试明确替换重型父类/RPC 边界，不证明真实 verl 初始化。
- integration：启动独立本地 CPU Ray，验证真实 GS 发现和句柄；不启动 GPU 或 verl 训练。
- native_unit：在具备配套 verl/vLLM 的环境中显式执行 `tests/native_unit/test_native_adapters.py`，验证真实父类；缺依赖会报错。
- 完整验收：用户用原生入口启动实际训练，检查相关 Actor/对象、生成、训练和参数同步。

没有完成完整训练前，不能把上述单元测试写成端到端成功。

## 代码入口

- [实体与初始化](docs/architecture.md)：创建者、持有者、原生复用点与代码位置。
- [后续验证](docs/development-plan.md)：已完成的接线与待验证范围。

固定 named GS 位于 namespace `verl-multi-task`，name 为 `verl-multi-task-group-scheduler`。
GS 采用 detached 生命周期；任务结束只解绑自己的句柄，不销毁其他任务正在使用的 GS。集群操作者负责最终清理 GS。
