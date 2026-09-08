# 原生入口配置与真实环境验收

状态：P1 接线已实现并由用户确认；真实 Hydra 启动和 GPU 训练待验证。
更新日期：2026-09-08。配套 verl 为 `a9ebd0bb`（`v0.9.0-5-ga9ebd0bb`），伴生源码/测试提交为 `3aa443d`/`9a2b785`。

## 1. 用户只感知一个扩展开关

用户继续从 `python -m verl.experimental.fully_async_policy.fully_async_main` 启动，
并设置 `multitask.runtime.profile=experimental_fully_async_standalone`。
该 Runtime Profile 选择整套 MultiTask 类型；用户不配置每个扩展类名，不使用伴生入口，也不复制 Hydra 主配置。

- 字段缺失或为 null：原生入口保留原生 TaskRunner，不导入伴生包，不发现 GS。
- 字段为上述 profile：原生入口选择 MultiTask TaskRunner，后续创建链选择下游扩展。
- 字段非法或显式启用后导入/配置失败：入口报错，不静默回退为原生训练。

原生入口读取 profile 的位置为 `verl/experimental/fully_async_policy/fully_async_main.py:238`；
主配置字段位于 `verl/experimental/fully_async_policy/config/fully_async_ppo_trainer.yaml:10`。
伴生校验器位于 `src/multi_task_scheduler/integration/verl/runtime_profile.py:28`。

## 2. 配置示例不是完整训练配置

[native_entry_overrides.txt](native_entry_overrides.txt) 只列接入和原生前提参数。
用户必须补充模型、数据路径、Trainer/rollout 节点与 GPU 数量、算法及训练步数等原生配置；
示例不能替代一个已经可运行的原生 Fully Async 任务。

当前 profile 要求 vLLM 非 PD、纯 STANDALONE。PD（Prefill/Decode disaggregation）指预填充与解码分离。
用户保持 `actor_rollout_ref.hybrid_engine=false`、
`async_training.use_trainer_do_validate=false` 和
`async_training.use_dynamic_resource_scheduling=false`，避免创建 HYBRID rollout。
用户提供正数 rollout 节点/GPU 数量，并选择非 naive Checkpoint Engine 后端。

profile 校验器只检查所选类型族、原生前提和资源总量能容纳至少一个 Replica；
verl 原生初始化仍校验后端、并行度、节点布局及其他运行限制。
GS 不分配初始化规模，用户继续通过原生 rollout 资源字段决定 Replica 数量。

## 3. 源码与配置的验收顺序

1. 用户在真实环境部署配套 verl 提交和同版伴生源码。用户将伴生源码放到 verl 仓根的
   `verl-multi-task/`，并向 driver、全部 Ray 节点和相关子进程提供 `verl-multi-task/src` 搜索路径。
   用户不能只凭 driver 导入成功判断 Actor 可以导入。
2. 用户检查原生 Hydra primary 的实际解析结果。用户确认 profile 字段和原生参数一起生效，
   而不是导入另一份训练主配置。配置输出检查本身不证明 Actor 初始化成功。
3. 用户先关闭 profile 运行原生基线，再用同一资源/模型/数据配置启用 profile。
   用户通过 Actor 类型和初始化日志核对九类 MultiTask 扩展、GS 及原生辅助组件。
4. 用户在启用 profile 的任务中分别设置 `async_training.partial_rollout=true` 和 `false`，
   验证原生生成、样本队列、训练更新、初始传权与至少一次后续传权。
   该开关仍由 verl 原生客户端处理；它不是另一个 MultiTask 类型选择器。
   若 true 场景没有触发实际中断和续推，用户只记录该开关下运行通过，不把续推路径标为已验。
5. 用户启动两个独立训练 job，确认两个任务发现同一个 GS，且一个任务退出不影响另一个任务。
   用户另行记录正常退出、初始化/训练失败和 GS 不可达时的结果。
   用户不要把强制 kill 后没有执行 finally 误判为已实现自动故障清理。

所有实验都应使用隔离的验收任务。用户保存完整命令、配置、两仓版本、运行环境、节点/GPU 数量、
各进程源码位置、Actor/对象类型和日志，再交给开发者审阅。

真实父类检查、各项完成条件与当前未验清单见 [开发计划](../../docs/development-plan.md)。
当前本地 58 项 unit 与 3 项 CPU Ray 测试不替代以上验证；本轮只更新文档，没有执行这些真实环境步骤。
