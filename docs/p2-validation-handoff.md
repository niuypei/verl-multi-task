# P2 验证交接：任务注册版本，待真实环境验证

状态：**P2 待真实环境验证**。本地检查已通过，但用户尚未返回真实环境结果。
更新日期：2026-09-10。本文记录已提交的 AS-IS 代码与待执行验证，不表示完整训练已经通过。

## 1. 固定验证版本

P2 指“原生入口接线 + 任务注册”的本次代码快照，不是旧计划中已撤回的 P2/Impl/SPI 改造。
P1 保留为历史接线快照，P1 同样没有真实环境通过结论。用户本次按 P2 验证，不使用持续变化的分支 HEAD。

| 仓库与用途 | 完整 commit ID | 说明 |
|---|---|---|
| MultiTask **P2 验证快照** | `aaab399bd970665480d72e678b08ff2aed66e719` | 包含 P1 接线、本次注册源码和选定测试；用户部署此版本的源码与测试 |
| MultiTask 注册源码提交 | `b392f807d4ae2f7b66569105adcd0bd749e97650` | 实际资源采集、注册、GS 初始资源视图及失败处理 |
| MultiTask 注册测试提交 | `aaab399bd970665480d72e678b08ff2aed66e719` | 资源数据、采集/接线、CPU Ray 测试及真实父类断言调整 |
| 配套 verl 验证快照 | `a9ebd0bb2354068229620b7e7a7aab2987edf864` | `v0.9.0-5-ga9ebd0bb`；本次注册没有新增 verl 修改 |
| verl 源码对比基线 | `adc7eefa16dad75c5f7b878823d5a76eac90c7b3` | 仅供 AST/源码契约测试使用，不是启用 MultiTask 的部署版本 |
| 历史 P1 快照 | `293d6fa2db10a103fadb71e0bd84f282833c1395` | 仅接线与 GS 发现，详见 [P1 交接](p1-validation-handoff.md) |

本文晚于 P2 验证快照。后续文档提交只补充交接，不改变上述源码/测试基线。
开发者在本地独立仓 `verl-multi-task/` 开发，配套源码位于同级 `verl/`。
用户在真实环境可将伴生源码放进 verl 目录，直接导入源码，无需先安装 wheel。

## 2. 范围与本地证据

P2 只覆盖 experimental Fully Async、纯 STANDALONE、vLLM 非 PD。
PD（Prefill/Decode disaggregation）指预填充与解码分离，本 profile 不支持该拓扑。
GroupScheduler（GS，全局调度器）保存任务的原生 rollout 节点/GPU 归属和训练节点；GS 尚不执行调度。
注册实现和代码行号见 [任务注册交付说明](task-registration.md#3-实际执行链与源码入口)。

| 验证层 | 2026-09-10 本地复验 | 证据边界 |
|---|---|---|
| `tests/unit/` | 187 passed | 纯数据、配置及 AST/源码合同；初始化顺序、RPC 和超时重试使用明确替身 |
| 选定 CPU Ray | 7 passed | 本机、单节点、同一 job 内的真实 Actor 查询/注册；GPU selectors 为合成元数据 |
| `tests/native_unit/test_native_adapters.py` | 11 项待执行 | 需要真实 verl 依赖；即使通过，也只证明继承/委托及两个本地构造场景 |
| 完整初始化、跨节点、GPU 训练 | 全部待用户验证 | 本地没有完整 verl/PyTorch/vLLM 环境，以上测试不证明这些路径通过 |

开发者复用现有虚拟环境，没有安装依赖，也没有执行 verl 全量测试或训练。
测试环境为 Python 3.12.14、Hydra 1.3.2、Ray 2.48.0；用户另行记录真实环境版本。
本地命令见 [复验命令](task-registration.md#6-已执行检查与复验命令)。

## 3. 部署与启动

1. 用户先准备能够运行原生 experimental Fully Async 的模型、数据、依赖和资源配置。
2. 用户按第 1 节固定提交准备配套 verl 与伴生源码，并记录来源、文件清单和校验值。
   用户将伴生源码和选定测试放入 verl 仓根的 `verl-multi-task/`，不覆盖未知文件，不复制 `.git`、虚拟环境、缓存或模型。
3. 用户向 driver、所有 Ray 节点、Actor 与相关子进程提供同版源码搜索路径，并核对实际模块路径。
   driver 导入成功不代表远端 Worker 能导入新增的 `resource_query` 回调或注册数据类。
4. 用户使用独立部署副本和隔离 Ray 环境。P2 GS 的 `runtime_kind()` 必须返回
   `verl-multi-task:experimental_fully_async_standalone:registration-v1`。
   name 仍为 `verl-multi-task-group-scheduler`，namespace 仍为 `verl-multi-task`。
   discovery 拒绝旧 P1 合同；操作者不得销毁或改写仍在服务 P1 的 GS 来绕过该错误。
5. 用户继续从 `python -m verl.experimental.fully_async_policy.fully_async_main` 启动，
   设置 `multitask.runtime.profile=experimental_fully_async_standalone`。
   注册没有新增开关；其他纯 STANDALONE 前提与原生参数见 [配置示例](../examples/experimental_fully_async/README.md)。

用户从真实环境的 verl 仓根，用已有训练环境执行以下选定真实父类测试：

```bash
PYTHONPATH="$PWD/verl-multi-task/src:$PWD" \
PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
python -m pytest -q -p no:cacheprovider \
  verl-multi-task/tests/native_unit/test_native_adapters.py
```

该命令不会运行整个 verl 测试集。依赖缺失或收集失败不能标为通过。
只有另行运行源码对比 UT 时，`MT_VERL_SOURCE_ROOT` 才必须指向含 `adc7eefa` Git 对象的 verl checkout；
这不是源码启动或上述真实父类测试的前提。

## 4. 真实环境验证计划

用户执行下表并保存证据，开发者收到结果后再定位修复。验证本身不计划新增 verl 或伴生功能。
每个子项分别填写“通过／失败／未执行／未覆盖”；当前所有真实环境子项均为待验证。

| 编号 | 用户执行与通过条件 | 当前状态 |
|---|---|---|
| P2-V1 固定版本与隔离 | 用户核对两个完整 SHA、部署清单、所有节点依赖版本，以及 P2 GS 合同和隔离环境 | 待验证 |
| P2-V2 真实类型与初始化 | 用户执行 11 项真实父类测试，并单独记录九类 MultiTask 扩展及 GS 的实际创建；用户核对原生 AgentLoop、MessageQueue、WorkerDict、ServerAdapter 正常初始化 | 待验证；测试与初始化分开记录 |
| P2-V3 入口与顺序 | 用户分别检查关闭、启用、非法 profile；用户用日志证明原生初始化、首次传权、可选初始化验证先于注册，注册确认先于 Trainer/Rollouter 两个 fit；有 checkpoint 时另验恢复路径 | 待验证；可选分支未触发则记未覆盖 |
| P2-V4 训练节点 | 用户从实际 actor/critic/ref 物理 Worker 核对节点与去重；用户确认结果不误用 controller 节点，不包含训练 GPU 明细或奖励/teacher 节点 | 待验证 |
| P2-V5 Rollout 拓扑 | 用户启动至少两个 native replicas，核对每个 Worker 的 node ID/GPU selectors 与每个 Server 的节点、replica rank、node rank、有序可见设备；单节点和跨节点 replica、非零起始 GPU 编号分别留证 | 待验证；缺少拓扑条件则记未覆盖 |
| P2-V6 GS 与跨 job | 用户启动两个使用同版源码、真实 GPU 分配不冲突的独立训练 job，确认共享 GS；用户核对同节点不同 GPU 归属、训练/rollout 两类节点关联及任务记录与派生视图一致 | 待验证 |
| P2-V7 幂等与失败 | 用户在隔离验收环境核对相同消息返回 ALREADY_REGISTERED、冲突无部分写入、采集/注册错误阻止 fit；超时仅重试相同消息一次，两次超时报提交结果未知 | 待验证；无法安全复现的故障记未覆盖 |
| P2-V8 原生流程与退出 | 用户对比 profile 关闭/启用后的生成、入队、取样、训练更新与后续传权；用户分别验证 partial 开/关，以及正常退出、初始化/训练异常只清理自己的记录，另一 job 继续运行 | 待验证；各分支分别留证 |
| P2-V9 结果交接 | 用户返回版本、配置、环境、拓扑、GS 快照、测试/训练日志与逐项结论；开发者按实际证据更新状态 | 待用户交接 |

P2-V2 的九类扩展为 `MultiTaskFullyAsyncTaskRunner`、`MultiTaskFullyAsyncTrainer`、`MultiTaskFullyAsyncRollouter`、
`MultiTaskLLMServerManager`、`MultiTaskCheckpointEngineManager`、`MultiTaskGlobalRequestLoadBalancer`、
`MultiTaskvLLMReplica`、`MultiTaskCheckpointEngineWorker` 和 `MultiTaskvLLMHttpServer`。
Manager/Replica 是 Rollouter 内普通对象；CE Manager 是 Trainer 内普通对象；其余上述扩展及 GS 是 Ray Actor。
Trainer 还接收序列化后的 Replica 同步投影。Ray Actor 清单不能证明这些普通对象的实际类型。

P2-V5 示例：rollout 使用 2 节点、每节点 8 GPU、每 replica 4 GPU 时，用户逐一核对 4 个 replica。
例如实际观察可能出现某个 replica 使用节点的 `4,5,6,7`；用户不能一律记录为 `0,1,2,3`，也不能把该例当作放置规则。
这个例子不覆盖跨节点 replica；用户需要另设符合原生后端约束的配置验证跨节点情况。

P2-V7 的冲突测试只提交测试元数据，不能通过重复分配真实 GPU 制造冲突。
生产 GS 不提供故障注入接口；用户仅在独立验收任务中按安全条件复现错误，不修改待验源码或扰动其他任务。
本地替身已检查响应丢失和未知提交结果，但真实链若未复现，用户仍将该子项记为未覆盖。

P2-V8 必须观察至少一次后续参数同步，不能仅凭首次传权成功判定通过。
`async_training.partial_rollout=true` 只表示开关开启；用户必须关联请求/样本、同步时刻和恢复输出，才能确认真实中断续推。
若实验没有发生中断，用户保留“中断/续推未覆盖”。强杀或 GS 不可达不保证 finally 执行，当前没有心跳清理或自动恢复。

## 5. 结果记录与排除范围

用户逐项复制以下记录模板，提交日志前删除密钥、令牌和不应共享的训练数据内容：

```text
验收项/子项：P2-V...
MultiTask commit：aaab399bd970665480d72e678b08ff2aed66e719
verl commit：a9ebd0bb2354068229620b7e7a7aab2987edf864
环境/部署清单/各进程源码位置：
完整命令/解析配置/profile/partial 值：
实际 Worker/Server 拓扑、GS 快照和时间关联日志：
结果：通过 / 失败 / 未执行 / 未覆盖
证据路径、异常栈、限制：
```

P2 不要求心跳、训练期间控制并发、空卡判断/上报、租借、动态 CE/LB、bootstrap 或调度策略。
注册及 detach 只维护元数据；用户不得将记录删除、请求为零或心跳超时解释为物理 GPU 释放。
开发者不会把 P2 运行结果回填成旧 P1 SHA 的验证结果，也不会将本地测试通过记为真实训练通过。
