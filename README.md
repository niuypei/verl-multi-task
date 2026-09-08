# verl-multi-task

状态：用户已确认 P1 接线代码；GS 发现与九类 MultiTask 创建链已实现，完整 verl/vLLM 运行待真实环境验收。
更新日期：2026-09-08。

本仓是独立的 Python 源码库。用户仍从
`verl.experimental.fully_async_policy.fully_async_main` 启动，不使用另一个伴生训练入口。

## 当前范围与版本

- 原生入口根据唯一的 `multitask.runtime.profile` 选择 MultiTask TaskRunner ActorClass。
  Runtime Profile 指一整套兼容扩展类型的选择，不替代原生训练配置。
- TaskRunner 创建或获取 GroupScheduler（GS，全局调度器）Ray Actor，GS 和 TaskRunner 互相持有句柄。
- 九类扩展覆盖 TaskRunner、Trainer、Rollouter、LLM Manager、LB、Replica、HTTP Server、CE Manager 和 CE Worker。
  LB 指请求负载均衡器，CE 指 Checkpoint Engine 参数同步组件。
- 扩展类复用原生 rollout、训练、MessageQueue、路由和参数同步。GS.schedule 返回空列表。
- 当前没有租约、心跳循环、借卡、bootstrap、同步 gate 或另一套 Replica registry。

当前适配 experimental Fully Async + 纯 STANDALONE + vLLM 非 PD，不是 V1。
PD（Prefill/Decode disaggregation）指预填充与解码分离；该路径需要另一组 Replica 类型，当前 profile 明确拒绝。
未启用 profile 时，verl 入口不导入本包，也不创建 GS。

当前配套提交：

| 仓库 | 配套提交 | 内容 |
|---|---|---|
| verl-multi-task | `3aa443d` | 接入源码与 GS 发现 |
| verl-multi-task | `9a2b785` | 选定测试及验证依赖 |
| verl-multi-task | `66e56dc` | 已有架构、部署与示例文档 |
| verl | `a9ebd0bb2354068229620b7e7a7aab2987edf864` | main 与主 YAML 两处接线；describe 为 `v0.9.0-5-ga9ebd0bb` |

表中伴生提交记录源码、测试和初版文档的来源，后续文档修订以本仓 Git 历史为准。
当前 verl 提交与先前配套提交 `2625ae0b` 的文件内容完全一致；本次提交检查没有修改 verl。
原生对比基线固定为 `adc7eefa16dad75c5f7b878823d5a76eac90c7b3`，不随接线提交移动。

## 源码部署与启动

本地独立开发源是 `verl-multi-task/src/multi_task_scheduler/`。用户在真实环境将伴生源码放到 verl 仓根下的
`verl-multi-task/`；该部署副本不改变本地独立仓的位置或版本管理。

用户应先核对目标目录，再放置源码、所选测试和示例。用户不覆盖已有 verl 文件，不复制子仓 `.git`、虚拟环境、缓存和模型数据。
用户应记录来源 commit、部署文件清单及校验值。所有 Ray 节点必须读取同版伴生源码与配套 verl。

以下命令只说明接入参数，**不是完整可运行的训练命令**。用户从真实环境的 verl 仓根执行，并使用已有的原生训练环境：

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

最后一行是占位说明，用户必须替换为自己的原生参数，不能原样执行。
源码嵌套与 driver 的 PYTHONPATH 不自动证明远端可导入；用户还要通过集群部署和 Ray runtime environment
向 Actor 及相关子进程提供正确搜索路径，并验证各进程实际导入的模块文件。接入代码不会修改业务模块的 sys.path。

部署必须包含配套 verl 接线提交；只复制本仓不能激活未修改的原生入口。
`multitask.runtime.profile=null` 或缺失字段保留原生路径；显式启用后，非法配置、缺失依赖或不支持的类型会报错。
用户继续使用 upstream Hydra primary，不配置逐类 FQN（Fully Qualified Name，完整限定类名）。
完整验收步骤见 [开发计划](docs/development-plan.md)；配置边界见 [示例说明](examples/experimental_fully_async/README.md)。

## 验证状态与命令

最近一次本地验证通过 58 项 unit 和 3 项真实 CPU Ray 检查。本轮文档更新没有重跑测试或安装依赖。
子仓虚拟环境使用 Python 3.12.14、Hydra 1.3.2、Ray 2.48.0；开发者没有安装完整 verl/PyTorch/vLLM 依赖。
默认测试目录只有 `tests/unit/`。

以下命令从伴生仓根执行，仅用于用户或开发者后续按需复核：

```bash
MT_VERL_SOURCE_ROOT=/absolute/verl \
PYTHONPATH="$PWD/src" PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
.venv/bin/python -m pytest -q -p no:cacheprovider tests/unit

PYTHONPATH="$PWD/src" PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 RAY_USAGE_STATS_ENABLED=0 \
.venv/bin/python -m pytest -q -p no:cacheprovider tests/integration/test_group_scheduler.py
```

| 测试层 | 当前结果 | 证明范围与限制 |
|---|---|---|
| unit | 58 项通过 | 真实 OmegaConf + AST/YAML 源码检查；重型父类与 RPC 使用替身，不证明完整初始化 |
| CPU Ray | 3 项通过 | 真实 GS 并发发现/句柄，测试专用父类的继承、序列化和远程执行；不等于真实 verl 父类或 GPU 验证 |
| native_unit | 11 项待真实环境执行 | 真实父类、方法委托、Ray 选项、TaskRunner/LB 本地构造；仍不启动 GPU Worker |
| 原生训练 | 待用户验证 | 原生 Hydra、跨节点导入、九类扩展与 GS 初始化、partial rollout 两种值、训练/同步、跨 job GS 共享与退出 |

源码对比测试通过 `git show` 读取固定原生基线；`MT_VERL_SOURCE_ROOT` 指向的 verl checkout 必须含有
`adc7eefa16dad75c5f7b878823d5a76eac90c7b3` 的 Git 对象。源码归档或不含该对象的浅克隆不能运行这部分检查。
该限制不影响直接源码启动，也不要求部署副本携带伴生 Git 元数据。

[开发计划](docs/development-plan.md) 提供真实父类测试命令、验收待办和结果记录要求。
缺少真实运行结果时，开发者不会把单元测试写成端到端训练成功。

## 代码与生命周期说明

[实体与初始化](docs/architecture.md) 列出九类扩展、GS、创建者、持有者、原生复用点和代码行号。
固定 GS 位于 namespace `verl-multi-task`，name 为 `verl-multi-task-group-scheduler`。
GS 使用 detached 生命周期；TaskRunner 正常进入退出清理时只解绑自己的句柄，不销毁共享 GS。
强制 kill、进程崩溃或 GS 不可达可能留下句柄；当前没有心跳恢复，集群操作者负责最终清理不再使用的 GS。
