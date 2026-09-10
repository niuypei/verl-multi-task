# 当前开发计划与验收待办

状态：**P2 待真实环境验证**。P2 包含 P1 接线与已获批的任务注册，不包含心跳或其他调度特性。
更新日期：2026-09-10。P1 保留为历史验证记录，不能因 P2 本地检查通过就标记 P1 已通过。

## 0. 当前执行计划：固定 P2 验证

用户本次部署 MultiTask `aaab399bd970665480d72e678b08ff2aed66e719` 与 verl
`a9ebd0bb2354068229620b7e7a7aab2987edf864`（`v0.9.0-5-ga9ebd0bb`）。
开发者已分别提交注册源码 `b392f807d4ae2f7b66569105adcd0bd749e97650` 与测试 `aaab399`，本次没有新增 verl 修改。
P2 是本次验证里程碑，不恢复旧 P2/SPI 施工方案；后续文档提交不移动 P2 验证快照。

| 顺序 | 当前待办 | 负责人及代码边界 | 状态 |
|---|---|---|---|
| 1 | 固定源码部署、隔离 P1、跨进程导入及真实类型检查（P2-V1/V2） | 用户/真实环境；部署既有两仓代码，不计划新增修改 | 待验证 |
| 2 | profile 关闭/启用/非法分支、初始化和注册时序（P2-V3） | 用户/真实环境；开发者根据日志核对 | 待验证 |
| 3 | 训练节点、各 replica 的 Worker/Server 实际 GPU 绑定、GS 与跨 job 视图（P2-V4—V6） | 用户/真实 GPU；元数据测试不能替代实际拓扑 | 待验证 |
| 4 | 幂等、冲突、有限重试、失败不进入 fit（P2-V7） | 用户/隔离验收环境；不新增生产故障注入接口，不干扰其他任务 | 待验证；未复现子项记未覆盖 |
| 5 | 原生训练、partial 开/关、后续传权及退出（P2-V8） | 用户/真实 GPU；不改变原生业务 | 待验证 |
| 6 | 返回证据、定位缺陷并更新结果（P2-V9） | 用户返回，开发者分析；修复另记提交，不改写固定快照 | 待交接 |

完整步骤、通过条件与结果模板见 [P2 验证交接](p2-validation-handoff.md)。
2026-09-10 本地复验为 187 项 unit、7 项 CPU Ray 通过；11 项真实父类测试及完整原生运行仍待执行。
源码实现和测试边界见 [注册交付说明](task-registration.md)。
以下第 1—5 节保留 P1 历史计划，旧 T1—T8 不替代本节的 P2-V1—V9。

## 1. P1 目标与固定验证版本

P1 验证目标是：用户从 verl 原生 experimental Fully Async 入口启用单一 Runtime Profile，
原生创建链实例化九类 MultiTask 扩展和 GroupScheduler（GS，全局调度器），扩展继续执行原生训练逻辑。
Runtime Profile 指选择一整套兼容扩展类型的配置值，不是另一套训练主配置。

- 伴生验证快照：`293d6fa2db10a103fadb71e0bd84f282833c1395`；源码提交为 `3aa443d`，测试提交为 `9a2b785`，初版文档提交为 `66e56dc`。
- 配套 verl：`a9ebd0bb2354068229620b7e7a7aab2987edf864`，describe 为 `v0.9.0-5-ga9ebd0bb`。
- 原生对比基线：`adc7eefa16dad75c5f7b878823d5a76eac90c7b3`。该基线不随接线提交移动。
- 当前实现只覆盖 experimental Fully Async、纯 STANDALONE、vLLM 非 PD；V1 和 PD 不属于本次验收范围。
  PD（Prefill/Decode disaggregation）指预填充与解码分离部署。

以上伴生提交记录初版交付来源，后续文档修订以本仓 Git 历史为准。
当前 verl 提交与先前配套提交 `2625ae0b` 的文件内容完全一致；固定原生对比基线不变。
用户按 [P1 验证交接](p1-validation-handoff.md)执行并填写结果；该文档集中记录完整 commit、命令和 T1—T8 证据要求。
开发者等待验证结果后再修改 P1；后续特性规划独立推进，不修改固定验收版本。

## 2. P1 已完成事项（固定版本历史记录）

| 阶段 | 已完成工作 | 负责人/环境 | verl 改动 | 验收证据 |
|---|---|---|---|---|
| P0 | 开发者撤回上一版接入实现，在独立伴生仓重新开发 | 开发者/本地 | 旧 Impl/SPI 补丁已撤回 | 当前类定义对比基线未变；P0 历史恢复副本仍保留在外层仓 |
| P1 入口 | 原生 main 按 profile 选择 MultiTask TaskRunner；未启用时保留原生类型 | 开发者/本地 | 仅 main 与主 YAML 两个文件；未修改原生类 | 当前配套提交 `a9ebd0bb` 与原接线提交 `2625ae0b` 内容一致；入口与配置源码检查 |
| P1 实体 | 开发者接通九类真实扩展及 GS，构造参数和原生业务继续传递 | 开发者/本地源码 | 无额外改动 | [实体与初始化](architecture.md) 列出各类创建点；源码提交 `3aa443d` |
| P1 清理 | 开发者删除 47 个未参与当前流程的协议、空服务和占位测试，没有新建备份 | 开发者/本地 | 无额外改动 | 当前代码不保留租约、bootstrap、同步 gate 或复制框架 |
| 本地选定测试 | 开发者通过 58 项 unit、3 项真实 CPU Ray 测试 | 开发者/子仓虚拟环境 | 测试只读取 verl 源码 | 最近一次执行结果为通过；本轮未重跑、未安装依赖 |

58 项 unit 使用真实 OmegaConf，但接线检查替换重型父类和 RPC；这组测试不证明真实 verl 初始化。
3 项 CPU Ray 测试验证 GS 并发发现、真实句柄，以及测试专用父类的解包/继承/远程执行；
这组测试不创建真实 MultiTask Trainer/Rollouter，也不启动 GPU。
本地环境是 Python 3.12.14、Hydra 1.3.2 和 Ray 2.48.0；开发者没有安装完整 verl/PyTorch/vLLM 依赖。

## 3. P1 历史验收计划

本表中的“无计划新增”表示本阶段只部署、运行和记录结果。如果测试暴露接线缺陷，开发者应先定位具体扩展点，
再提交最小修复供审视；开发者不能借验证重写原生类或提前增加业务。

| 待办 | 负责人/环境 | 伴生仓工作 | 是否修改 verl | 完成条件与证据 |
|---|---|---|---|---|
| T1 配套源码部署 | 用户执行，开发者提供说明/真实环境 | 用户将同版伴生源码嵌入 verl，并记录提交与文件清单 | 用户部署已有接线提交；无计划新增 | driver、所有 Ray 节点和子进程使用同版源码；部署记录没有 Git、虚拟环境或缓存副本 |
| T2 真实父类检查 | 用户执行，开发者分析失败/具备原生依赖的环境 | 执行现有 `tests/native_unit/test_native_adapters.py` 的 11 个用例 | 无计划新增 | pytest 原始输出通过；真实父类、方法委托、Ray 资源选项和两个本地构造用例均有结果 |
| T3 原生配置加载 | 用户执行，开发者核对/原生 Hydra 入口环境 | 核对 profile 未启用、启用、非法值三条入口路径 | 无计划新增 | 原生 primary 加载成功；关闭时不导入伴生包/不发现 GS；启用错误不回退原生 |
| T4 跨 Actor 源码来源 | 用户采集，开发者核对/实际 Ray 集群 | 核对 TaskRunner、Trainer、Rollouter、LB、CE Worker、Server 进程的模块文件与版本 | 无计划新增 | 各进程引用部署清单中的源码；没有旧包覆盖或只在 driver 可见的源码 |
| T5 全部实体初始化 | 用户执行，开发者审阅/实际 GPU 集群 | 检查 [实体清单](architecture.md#1-实体所有者和创建点) 的九类扩展与 GS；同时检查原生 AgentLoop、队列、WorkerDict、ServerAdapter | 无计划新增 | Actor 状态、对象类型和初始化日志能互相印证；GS 保存当前 TaskRunner 句柄；Trainer/CE 与 Rollouter/Manager 所有权正确 |
| T6 原生训练保持 | 用户执行，开发者对比/实际 GPU 集群 | 对同一可运行原生配置分别关闭/启用 profile；启用时分别验证 `partial_rollout=true/false` | 无计划新增 | 原生初始传权、生成、样本入队、取样更新和后续传权均正常；真实样本/训练指标和失败日志可审阅 |
| T7 退出与失败边界 | 用户执行，开发者核对/独立验收任务 | 检查两个训练 job 共享一个 GS，以及正常退出、初始化失败、训练异常和 GS 不可达时的行为 | 无计划新增 | 两个 job 取得同一 GS；一个任务退出不影响另一个任务；finally 解绑自己的句柄；清理失败不掩盖原异常 |
| T8 验收归档 | 用户提供结果，开发者更新文档 | 归档上述版本、命令、日志与通过/失败/未执行项 | 无计划新增 | 每个待办有实际证据；未通过项目仍保留为待办，不以测试数量代替训练验收 |

T2 的 11 个用例不是 11 个完整初始化场景。测试只导入真实父类、检查继承与委托，并在本地构造 TaskRunner 和 LB；
测试不会启动 GPU Worker。T5、T6 仍必须单独执行。

T6 中，用户保持 `async_training.use_trainer_do_validate=false` 和
`async_training.use_dynamic_resource_scheduling=false`，避免引入 HYBRID rollout。
`async_training.partial_rollout` 是可分别验证的原生开关，不是 MultiTask profile 开关。
开发者应在训练中观察至少一次后续参数同步；只有初始传权成功不足以验收异步流程。
若 partial_rollout=true 的实验没有实际触发生成中断和续推，开发者只记录该开关下运行通过，不声称续推路径已经验证。

T7 不要求实现心跳或故障恢复。TaskRunner 被直接杀死、进程崩溃或 GS 无法访问时，Python finally 不保证执行成功；
当前 GS 不自动清理遗留句柄。用户应在隔离验收集群记录该限制，并由集群操作者清理不再使用的 detached GS。
本地并发发现测试只覆盖同一 Ray job 中的并发调用，不能代替两个真实训练 job 之间的共享与退出验证。

## 4. P1 历史真实环境验证步骤

1. 用户先准备已经能够运行原生 experimental Fully Async 的依赖、模型、数据和资源配置。
   开发者不要求在本地笔记本安装完整训练依赖。
2. 用户按 [源码部署说明](../README.md#源码部署与启动) 放置同版源码，并确认每个 Ray 节点的搜索路径。
3. 用户从真实环境 verl 仓根选择现有真实父类测试，不运行整个 verl 测试集：

   ```bash
   PYTHONPATH="$PWD/verl-multi-task/src:$PWD" \
   PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
   python -m pytest -q -p no:cacheprovider \
     verl-multi-task/tests/native_unit/test_native_adapters.py
   ```

4. 用户使用 [原生入口示例](../examples/experimental_fully_async/README.md) 验证原生 Hydra 配置和训练。
   用户为启用/关闭 profile、partial rollout 两种值保存独立配置及日志。
5. 用户把结果交给开发者。开发者按 T1—T8 更新状态并修复 P1 接线缺陷；开发者把特性需求另列为 F1—F6。

若用户另行运行 `tests/unit/` 的源码对比，`MT_VERL_SOURCE_ROOT` 必须指向真实 verl checkout；
该 checkout 必须包含固定基线 `adc7eefa16dad75c5f7b878823d5a76eac90c7b3` 的 Git 对象。
纯源码归档或不含该对象的浅克隆不能执行这部分 `git show` 检查。这个限制不影响源码启动本身，也不是 T2 的运行前提。

每次验收记录至少包含：两个源码 commit、文件清单/校验值、Python/Ray/verl/vLLM/CUDA 版本、节点与 GPU 数量、
完整启动命令、解析后的配置、参与进程的源码位置、日志位置、各待办的结果及异常信息。

## 5. P1 验证不包含的事项

P1 验证不要求心跳、空闲识别、租约、GPU 捐赠/受赠、动态 Replica、target-only bootstrap、同步 gate 或调度策略。
后续阶段也不新增伴生训练入口、逐类配置、镜像 Hydra primary、Impl/SPI 改造或通用复制框架。

## 6. 后续特性计划（TO-BE，待评审）

本节 F1—F6 保留为历史候选，不是最新施工顺序。用户已改为五类职责，并选择先推进任务注册。
当前注册已实现；后续顺序为主动心跳及训练期间心跳控制并发、空卡接收、全局租借视图、指令回执、完整注销，接口随功能按需设计。

用户验证 P1 与开发者规划特性并行。F 表示 Feature，F1—F6 不复用此前已撤回的 P2/P3 大骨架计划。
首个闭环建议仍限定 experimental Fully Async、纯 STANDALONE、vLLM 非 PD；建议先验证 NCCL 后端。

| 阶段 | 交付目标 | verl 改动边界 |
|---|---|---|
| F1 资源登记与控制入口 | TaskRunner 在训练中响应 GS 心跳；Manager 查询实际 node/GPU，TaskRunner 向 GS 登记；GS 维护任务资源与注销 | 预计只改子仓；先验证 control 并发通路，不改原生 run |
| F2 空泡观测 | Rollouter、LB、server 汇合生产窗口与请求状态，LB 上报候选及撤销；不自动借卡 | 预计子仓覆写/组合；不改变原生陈旧度和样本规则 |
| F3 技术前置验证 | 真正 STANDALONE sleep/wake、borrower 自有 receiver/server 直接创建、已发布权重重放与通信组重建 | 优先子仓；权重来源或创建合同无法覆盖时，先评审必要窄接线 |
| F4 手动借还闭环 | GS 指定操作；donor 摘流/CE 排除/sleep，borrower 创建/追平/CE ADD/LB 接流，最后安全归还 | 依赖 F3；子仓维护单集合、单 replica-sync gate 与跨 Actor 回执 |
| F5 强制回收 | 定向 abort、跨实例续推、请求唯一完成和失败隔离；分别评审 partial 开/关 | 优先新增客户端子类和既有选择点；不直接修改原生业务实现 |
| F6 自动策略 | 对接模拟器决策，先只记录计划，再自动执行并验证共享收益 | 预计 GS 策略与配置为主，不预设 core 改造 |

F3 可以与 F1/F2 的无 GPU 工作并行。F4 不能在物理释放、独立 receiver 或版本重放尚未证明时启用真实借卡。
F4 的自然排空不承诺强制回收时限；donor 必须等待 borrower 实际释放后才能 wake。
新特性不能连接并改写 P1 验证任务正在使用的 GS。开发者分别记录 P1 修复与特性提交，不用变化的 HEAD 替代固定验证快照。

当前用户先按第 0 节与 P2 交接验证注册快照；主动心跳与控制并发需单独细化，开发者不自动实施旧 F1 全部内容。
