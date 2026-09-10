# P1 验证交接：固定版本与待验证事项

状态：P1 接线代码已确认；真实环境验证待用户执行。本文记录 AS-IS 交付，不宣称原生训练已经通过。
更新日期：2026-09-08。本轮只编写交接说明，没有运行测试、安装依赖或修改源码。

用户使用本节固定版本验证 P1，开发者根据用户返回的证据修复接线问题。
后续特性规划与 P1 验证并行；用户不要用持续变化的分支 HEAD 替代本次验收版本。

## 1. 固定交付版本

开发源码位于 `/Users/nyp/Documents/multi_task_verl/verl-multi-task` 与同级 `verl`。
真实环境可以采用源码嵌套布局；用户仍分别记录两个仓库的版本。

| 仓库与用途 | 完整 commit ID | 内容 |
|---|---|---|
| MultiTask 验证快照 | `293d6fa2db10a103fadb71e0bd84f282833c1395` | 包含以下源码、测试、初版文档和后续文档更新 |
| MultiTask 源码提交 | `3aa443d17ccdeb232ce71b1e892f80fad251f774` | 原生入口适配、九类扩展与 GS 发现 |
| MultiTask 测试提交 | `9a2b78596659c0d1dcaaac1cf8ccaa1f99d96482` | 分层测试与验证依赖声明 |
| MultiTask 初版文档 | `66e56dcab18f6fc87948e237eebf42c2dcdff718` | 实体、源码部署与示例 |
| 配套 verl 验证快照 | `a9ebd0bb2354068229620b7e7a7aab2987edf864` | 原生 main 与主 YAML 接线；describe 为 `v0.9.0-5-ga9ebd0bb` |
| verl 原生对比基线 | `adc7eefa16dad75c5f7b878823d5a76eac90c7b3` | 源码契约测试读取的固定历史基线，不是启用 MultiTask 的部署版本 |

当前 verl 快照与历史接线提交 `2625ae0b468a23d87c88fd5a3f08b5c5210415ad` 的文件内容一致。
用户本次以 `a9ebd0bb` 记录验收版本，不把旧提交号当成当前 HEAD。
本文本身晚于上述 MultiTask 快照；本文不改变 P1 源码和测试基线。

## 2. 本次验证范围与已有证据

P1 只覆盖 experimental Fully Async、纯 STANDALONE、vLLM 非 PD。
PD（Prefill/Decode disaggregation）指预填充与解码分离，当前 profile 不支持该拓扑。
Runtime Profile 指选择整套兼容扩展类型的单一配置值，不是一份新训练主配置。
GroupScheduler（GS，全局调度器）实际创建并保存任务句柄；新增资源共享业务仍为空。

| 已有验证层 | 最近记录 | 不能据此宣称的结果 |
|---|---|---|
| unit | 58 项通过；真实 OmegaConf、AST/YAML 检查，重型父类和 RPC 使用替身 | 真实 verl 父类或完整 Actor 初始化通过 |
| CPU Ray | 3 项通过；真实 GS 发现与句柄、测试专用父类的解包/继承/远程调用 | 真实 MultiTask Trainer/Rollouter 或两个训练 job 已运行 |
| native_unit | 11 项待执行；真实父类检查及 TaskRunner/LB 本地构造 | 全部 Actor、GPU Worker 或 GPU 训练通过 |
| 原生训练 | 待用户验证 | Hydra、跨节点导入、参数同步和训练循环已经通过 |

上述已通过结果来自此前本地执行，环境为 Python 3.12.14、Hydra 1.3.2、Ray 2.48.0。
本地没有完整 verl/PyTorch/vLLM 环境。用户保留现有可运行训练环境，并记录实际依赖版本。
测试定义见 `tests/native_unit/test_native_adapters.py:33`、`:48`、`:63`、`:71`。

## 3. 用户执行顺序

### T1：部署配套源码

用户先准备已经能运行原生 experimental Fully Async 的模型、数据、依赖和资源配置。
用户将固定版伴生源码放到真实环境 verl 仓根的 `verl-multi-task/`，并记录文件清单与校验值。
用户不覆盖原有文件，不复制伴生 `.git`、虚拟环境、缓存或模型数据。
用户将 `verl-multi-task/src` 与 verl 仓根加入 driver、所有 Ray 节点及相关子进程的源码搜索路径。
目录嵌套或 driver 导入成功都不能证明远端 Actor 能够导入同版源码。
具体布局见 [源码部署说明](../README.md#源码部署与启动)，用户不需要先安装 wheel 或 editable 包。

### T2：执行真实父类检查

用户从真实环境 verl 仓根、使用已有训练环境的 Python 执行下列选定测试，不运行整个 verl 测试集：

```bash
PYTHONPATH="$PWD/verl-multi-task/src:$PWD" \
PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
python -m pytest -q -p no:cacheprovider \
  verl-multi-task/tests/native_unit/test_native_adapters.py
```

用户保存完整 pytest 输出；依赖缺失或收集失败属于未通过，不以替身结果替代。
11 个用例检查真实继承、方法委托、Ray 选项以及两个本地构造场景，不创建 GPU Worker。
若用户另行执行 `tests/unit/` 的源码对比，用户提供的 verl checkout 必须含有 `adc7eefa` 的 Git 对象；
该要求只针对源码对比测试，不是源码启动或上述 native_unit 的前提。

### T3：验证原生入口与配置

用户继续执行 `python -m verl.experimental.fully_async_policy.fully_async_main`，使用原生 Hydra 主配置。
用户将以下覆盖项合入已有可运行命令；本表不是完整训练配置。

| 配置字段 | 启用 P1 时的取值 |
|---|---|
| `multitask.runtime.profile` | `experimental_fully_async_standalone` |
| `actor_rollout_ref.hybrid_engine` | `false` |
| `actor_rollout_ref.rollout.name`、`.mode` | `vllm`、`async` |
| `actor_rollout_ref.rollout.checkpoint_engine.backend` | 当前训练环境可用的非 `naive` 后端，例如已验证可用的 `nccl` |
| `actor_rollout_ref.rollout.calculate_log_probs` | `true` |
| `async_training.use_trainer_do_validate` | `false` |
| `async_training.use_dynamic_resource_scheduling` | `false` |
| `data.train_batch_size`、`data.gen_batch_size` | `0`、`1` |
| `rollout.nnodes`、`rollout.n_gpus_per_node` | 用户填写正数，并与原生并行度满足后端初始化要求 |

原生 main 读取 profile 的位置是 `verl/experimental/fully_async_policy/fully_async_main.py:243`；
原生主配置定义位于 `verl/experimental/fully_async_policy/config/fully_async_ppo_trainer.yaml:10`。
伴生校验器位于 `src/multi_task_scheduler/integration/verl/runtime_profile.py:28`；校验器不替代后端拓扑校验。
用户分别验证三个分支：`profile=null` 保留原生类型且不发现 GS；合法 profile 选择扩展；
非法 profile 或启用后的导入失败明确报错、不回退为原生训练。用户保存实际解析配置及每个分支的输出。

### T4—T5：核对各进程源码与全部实体

用户记录每个参与进程的 node ID、PID、Actor ID、实际类名和模块文件路径。
ActorHandle 是远程 Actor 的引用；普通对象不会因其持有 ActorHandle 就变成 Ray Actor。
下表列出九类 MultiTask 扩展及 GS。表中源码路径均相对 `src/multi_task_scheduler/`。

| 实体 | 用户应观察到的运行实体/所有者 | 源码位置 |
|---|---|---|
| `GroupScheduler` | 全局 Ray Actor；TaskRunner 与 GS 相互持有句柄 | `scheduler/group_scheduler.py:9` |
| `MultiTaskFullyAsyncTaskRunner` | driver 创建的 Ray Actor | `integration/verl/experimental_fully_async/task_runner.py:34` |
| `MultiTaskFullyAsyncTrainer` | TaskRunner 创建的 Ray Actor | `integration/verl/experimental_fully_async/trainer.py:26` |
| `MultiTaskFullyAsyncRollouter` | TaskRunner 创建的 Ray Actor；Trainer 也持有句柄 | `integration/verl/experimental_fully_async/rollouter.py:20` |
| `MultiTaskLLMServerManager` | Rollouter 内普通对象 | `integration/verl/experimental_fully_async/llm_server_manager.py:12` |
| `MultiTaskCheckpointEngineManager` | Trainer 内普通对象 | `checkpoint/checkpoint_engine_manager.py:6` |
| `MultiTaskGlobalRequestLoadBalancer` | Manager 创建的 Ray Actor | `rollout/load_balancer.py:6` |
| `MultiTaskvLLMReplica` | Manager 内普通对象；Trainer 取得序列化副本和其中的句柄 | `rollout/replica.py:13` |
| `MultiTaskCheckpointEngineWorker` | Replica 经 RayWorkerGroup 创建的 Ray Actor | `checkpoint/checkpoint_engine_worker.py:6` |
| `MultiTaskvLLMHttpServer` | Replica 创建的 Ray Actor | `rollout/http_server.py:6` |

用户核对 TaskRunner → Rollouter → Manager → LB 保存同一 GS 句柄；Trainer/CE 不持有 GS。
用户同时检查原生 `FullyAsyncAgentLoopManager`、`FullyAsyncLLMServerClient`、`MessageQueue`、
`MessageQueueClient`、`WorkerDict` 与 `ServerAdapter` 正常参与，不要求这些原生实体改名。
详细创建链和原生调用行号见 [实体与初始化](architecture.md#2-初始化调用流程)。
用户可以结合 Ray 状态、已有日志与调试器观察实际对象。Ray Actor 清单不能证明 Manager/Replica 等普通对象类型；
用户若无法获取普通对象类型或远端模块路径，用户应把对应项记录为“未覆盖”，不要据此判定通过。

### T6：验证原生训练与 partial rollout

1. 用户用同一模型、数据、资源和算法配置，分别运行 profile 关闭与启用两组任务。
2. 用户在启用组分别设置 `async_training.partial_rollout=false` 和 `true`，保存独立结果。
3. 用户确认初始参数同步、生成、样本入队、Trainer 取样、训练更新及至少一次后续参数同步均有实际记录。
4. 用户在 `true` 组记录一次真实生成中断与续推，并关联请求/样本标识、同步时刻和恢复输出。
   若该次实验没有发生中断，用户只标记“开关开启后训练通过”，把中断/续推子项保留为“未覆盖”。

partial rollout 指原生请求中断后的续推，不表示用半条 trajectory 执行 PPO 更新。
用户不以成功构造组件、首次传权成功或 mock 调用次数替代上述训练证据。

### T7：验证跨 job GS 共享与退出边界

用户在隔离验收集群启动两个独立训练 job，并确认两个 job 发现同一 GS、分别登记自己的 TaskRunner。
GS 的 namespace 是 `verl-multi-task`，name 是 `verl-multi-task-group-scheduler`；
GS 的 `runtime_kind()` 应返回 `verl-multi-task:experimental_fully_async_standalone:p1`。
GS 的 `get_task_runners()` 返回登记句柄；定义见 `scheduler/group_scheduler.py:20`、`:39`。
用户结束其中一个任务，确认 GS 只移除该任务句柄，另一个任务继续运行。
用户另行记录初始化失败、训练异常和 GS 不可达时的原始异常与清理日志。
TaskRunner 的 finally 解绑位于 `integration/verl/experimental_fully_async/task_runner.py:50`。
强制 kill、进程崩溃或 GS 不可达不保证 finally 成功；当前代码没有心跳清理或自动恢复。
用户不要在其他任务使用 GS 时销毁 GS；集群操作者负责隔离验收后的最终清理。

### T8：填写并返回结果

用户按下表填写“通过／失败／未执行／未覆盖”，并附证据位置；开发者收到结果后再修改 P1。

| 待办 | 用户结果 | 证据路径或失败摘要 |
|---|---|---|
| T1 同版源码部署与文件清单 | 待用户验证 | |
| T2 11 项真实父类检查 | 待用户验证 | |
| T3 关闭／启用／非法 profile | 待用户验证 | |
| T4 各进程模块来源 | 待用户验证 | |
| T5 九类扩展、GS 与原生辅助实体初始化 | 待用户验证 | |
| T6 原生对比、partial=false/true、实际中断续推、后续传权 | 待用户验证；各子项分别记录 | |
| T7 两个 job 共用 GS、退出与失败边界 | 待用户验证；各子项分别记录 | |
| T8 证据交接与结论归档 | 待用户交接 | |

用户返回的最小材料包括：两仓 commit、部署清单/校验值、Python/Ray/verl/vLLM/CUDA 版本、节点与 GPU 数量、
完整命令、解析后的配置、进程/Actor 与普通对象类型记录、模块位置、pytest 输出、训练及异常日志。
用户在每条训练日志中注明 profile 与 partial rollout 取值，并记录初始/后续传权及真实中断续推是否发生。
用户发送日志前删除密钥、访问令牌和不应共享的训练数据内容，但保留定位问题所需的错误栈与时间关联。
开发者将 P1 缺陷与后续特性需求分别记录；开发者不把未实现的心跳、借卡、bootstrap 或调度能力列为 P1 验收失败。
