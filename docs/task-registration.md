# 任务注册：实现交付与验收说明

状态：注册源码与测试已提交，记为 **P2 待真实环境验证**。
更新日期：2026-09-10。本文描述 AS-IS 实现，不替代后续资源共享设计。

## 1. 使用范围与版本

本功能覆盖 experimental Fully Async、纯 STANDALONE、vLLM 非 PD。
PD 指 Prefill/Decode disaggregation，即预填充与解码分离。
GroupScheduler（GS，全局调度器）现在保存任务初始资源归属，但仍不执行调度。

- 配套 verl：`a9ebd0bb2354068229620b7e7a7aab2987edf864`，`v0.9.0-5-ga9ebd0bb`。
- MultiTask P2 验证快照：`aaab399bd970665480d72e678b08ff2aed66e719`，包含注册源码与测试。
- 注册源码提交：`b392f807d4ae2f7b66569105adcd0bd749e97650`；P1 基础提交仍为 `293d6fa2db10a103fadb71e0bd84f282833c1395`。
- 本功能没有新增 verl core 修改，没有新增 Hydra 配置，也没有改变原生训练入口。
- [固定 P1 验证交接](p1-validation-handoff.md)及其验收版本保持不变，尚无真实环境通过结论；用户本次按 [P2 验证交接](p2-validation-handoff.md)执行。

用户继续执行 `python -m verl.experimental.fully_async_policy.fully_async_main`，
并设置既有 `multitask.runtime.profile=experimental_fully_async_standalone`。
用户沿用[原生参数说明](../examples/experimental_fully_async/README.md)，补齐自己的模型、数据和资源配置。
注册没有单独开关；启用上述 profile 后，新版本 TaskRunner 自动执行资源注册。

## 2. 部署时先隔离 P1

当前 GS 的 `RUNTIME_KIND` 为 `verl-multi-task:experimental_fully_async_standalone:registration-v1`。
GS discovery 仍使用名称 `verl-multi-task-group-scheduler` 和命名空间 `verl-multi-task`。
discovery 在发现旧 P1 合同后明确报错，不替换或升级旧 Actor：
[discovery.py](../src/multi_task_scheduler/scheduler/discovery.py):21。

用户必须在独立部署副本和隔离的 Ray 环境中验证注册版本。
操作者不得为了消除合同错误而销毁、清空或替换正在服务 P1 验收的 named GS。
所有参与注册测试的任务必须使用同版 MultiTask 源码；GS 不负责自动协调混用版本。

用户按[源码部署说明](../README.md#源码部署与启动)配置源码搜索路径，无需先安装 wheel。
driver、各 Ray 节点的 Actor 和相关子进程都必须能导入同版伴生源码与兼容 verl。
只让 driver 导入成功，不足以证明远端 Worker 能导入新增查询函数。

## 3. 实际执行链与源码入口

下表的行号对应上述 P2 验证快照；后续改动可能移动行号。
TaskRunner、Trainer、Rollouter 和 GS 是 Ray Actor；Manager 是 Rollouter Actor 内的普通对象。

| 执行者与动作 | 当前源码 |
|---|---|
| TaskRunner 先向 GS attach 控制句柄，并保留既有 finally 清理 | [task_runner.py](../src/multi_task_scheduler/integration/verl/experimental_fully_async/task_runner.py):44 |
| TaskRunner 调用父类完整初始化，返回后并行请求两类元数据 | [task_runner.py](../src/multi_task_scheduler/integration/verl/experimental_fully_async/task_runner.py):59 |
| Trainer 从自身 `all_wg` 发起训练节点采集 | [trainer.py](../src/multi_task_scheduler/integration/verl/experimental_fully_async/trainer.py):31 |
| Rollouter 把采集请求交给自身普通 Manager | [rollouter.py](../src/multi_task_scheduler/integration/verl/experimental_fully_async/rollouter.py):28 |
| Manager 查询自身主 STANDALONE replicas | [llm_server_manager.py](../src/multi_task_scheduler/integration/verl/experimental_fully_async/llm_server_manager.py):23 |
| 查询模块在目标 Actor 中读取节点、Worker 设备和 Server 绑定 | [resource_query.py](../src/multi_task_scheduler/integration/verl/resource_query.py):23 |
| TaskRunner 汇总两份结果，提交注册并核对成功返回值 | [task_runner.py](../src/multi_task_scheduler/integration/verl/experimental_fully_async/task_runner.py):72 |
| GS 校验全部元数据和跨任务 GPU 冲突后一次写入 | [group_scheduler.py](../src/multi_task_scheduler/scheduler/group_scheduler.py):48 |

父类初始化包含模型/队列创建、checkpoint 恢复、首次参数同步和可选 `val_before_train` 验证。
原生 `fully_async_main.py:46` 先执行初始化，`:186` 的训练循环随后才提交两个 `fit.remote()`。
因此，TaskRunner 在注册成功前不会启动训练循环；注册失败也不会跳过父类初始化后直接进入 fit。

## 4. GS 保存什么，不保存什么

GS 的 `task_runners` 继续保存 TaskRunner ActorHandle。
新增 `task_resources` 是资源元数据的唯一内存记录，按 task ID 保存一个不可变注册对象；GS 重启不会自动恢复这些记录。
四个 frozen 数据类定义于 [registration.py](../src/multi_task_scheduler/scheduler/registration.py):16。
注册对象包含节点目录、训练节点 ID 和 native replica 的有序节点/GPU 对应关系。
这里 native replica 指任务按自身初始资源创建的推理实例。

Trainer 只查询实际存在的 actor、critic、ref 物理 Worker，并按 node ID 去重。
GS 暂时只保存训练节点；GS 不从节点数量推导训练 GPU 明细、空闲状态或可借权限。
controller、奖励模型和 teacher 的节点不属于本次训练节点清单。
训练节点与 rollout 节点可以相同；GS 保留两类关联，不把节点角色设为互斥关系。

Manager 从每个 rollout Worker 读取实际 Ray GPU selector，并核对连续 Worker 块与 HTTP Server 的节点、rank 和可见 GPU 顺序。
GPU 归属键是 `(node_id, gpu_id)`；GPU selector 不自动等于硬件 UUID 或进程内 `cuda:0`。
GS 不接收 Worker、Server、Trainer、Rollouter 句柄，也不接收 ResourcePool、WorkerGroup 或完整配置。
注册不改变 Checkpoint Engine 参数同步组件、负载均衡器或 Manager 的原生有效实例集合。

`get_task_resources(task_id)` 返回独立、不可变的记录副本或 `None`。
`get_resource_view()` 按需生成新的普通字典，不维护第二份可写资源索引：
[registration.py](../src/multi_task_scheduler/scheduler/registration.py):173。

| 派生字段 | 内容 |
|---|---|
| `tasks[task_id]` | 该任务的纯字典资源记录 |
| `nodes[node_id].node_id`、`.node_ips` | 节点主键及登记中出现过的非空定位地址 |
| `nodes[node_id].training_task_ids` | 在该节点登记训练 Worker 的任务 ID 列表 |
| `nodes[node_id].rollout_owners` | 含 task ID、replica rank、node rank 和有序 GPU IDs 的节点块列表 |
| `nodes[node_id].gpu_owners[gpu_id]` | 对应 native GPU 的 task ID、replica rank 和 node rank |

不同任务的地址观察值可以共存于 `node_ips`；GS 只用 node ID 判断资源身份。
视图中缺失的 GPU 是未知资源，不是空闲资源。注册成功也不表示显存释放、任务持续健康或 GPU 可借出。

操作者在已连接本次验证 Ray 集群的 Python 进程中执行以下只读查询，不调用 discovery 创建另一个 GS：

```python
import ray

gs = ray.get_actor("verl-multi-task-group-scheduler", namespace="verl-multi-task")
view = ray.get(gs.get_resource_view.remote(), timeout=30)
print(view["tasks"])
print(view["nodes"])
```

## 5. 等待、重试和退出

TaskRunner 对外层采集和每次注册等待设置 30 秒期限；Trainer/Manager 内部批量查询也设置 30 秒期限。
这些期限不限制模型初始化、首次传权或初始化验证的运行时间。
只有注册 RPC 的 `GetTimeoutError` 会触发一次重试；TaskRunner 重用完全相同的不可变消息，不重新采集拓扑。
GS 对同任务相同记录返回 `ALREADY_REGISTERED`；GS 拒绝同任务不同记录及跨任务重复 GPU 归属。
校验失败、其他 RPC 异常或错误返回值都不会触发这次超时重试。

注册两次都超时时，TaskRunner 报告提交结果未知并终止初始化，不宣称 GS 没有提交。
采集超时会结束本地等待；实现不强杀已经提交的只读 Actor 工作，也不因此允许进入 fit。
既有 finally 调用 `detach_task()`，GS 同步移除本任务句柄和资源记录，不销毁 Worker 或释放物理 GPU。
detach 失败只记录警告，不掩盖原始异常；TaskRunner 被强杀时，finally 可能不执行。
没有心跳的本版本可能留下旧登记，操作者必须核对冲突，不能把超时或旧记录删除当成物理释放证明。

## 6. 已执行检查与复验命令

用户在子仓目录使用专用虚拟环境执行选定测试，不运行整个 verl 测试集。
开发者于 2026-09-10 复用已有虚拟环境重新通过下列本地检查，没有安装任何依赖或完整 verl/PyTorch/vLLM 训练环境。

```bash
MT_VERL_SOURCE_ROOT=/absolute/path/to/verl \
PYTHONPATH=src PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
.venv/bin/python -m pytest -q -p no:cacheprovider tests/unit

PYTHONPATH=src PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 RAY_USAGE_STATS_ENABLED=0 \
.venv/bin/python -m pytest -q -p no:cacheprovider \
  tests/integration/test_group_scheduler.py tests/integration/test_task_registration.py
```

| 验证层 | 本轮记录与边界 |
|---|---|
| unit | 187 passed；纯元数据、源码合同和明确使用父类/RPC 替身的接线测试 |
| CPU Ray | 7 passed；测试使用隔离的真实 Ray Actor，GPU selectors 是合成元数据 |
| 真实 verl 父类与 GPU 运行 | 待用户验收；本地结果不证明真实 Trainer/Server 初始化、跨节点放置或传权通过 |

CPU Ray 用例覆盖真实注册序列化、幂等、冲突、清理、异步 Actor 查询和旧 GS 合同拒绝。
异常与超时用例只验证查询边界；这些用例没有启动真实 verl Worker 或 vLLM HTTP Server。

## 7. 用户真实环境验收

以下步骤全部待验证；[P2 验证交接](p2-validation-handoff.md#4-真实环境验证计划)细化 P2-V1—V9、部署要求与结果模板。

1. 用户在隔离环境记录配套 verl 提交、当前 MultiTask 文件清单/校验值及实际 Ray/vLLM 版本。
2. 用户从原生入口启动至少含两个 native replicas 的任务，确认首次传权及可选验证先于注册，注册成功先于 fit。
3. 用户核对训练 Worker 实际节点，以及每个 rollout Worker 的 GPU selectors、Server 节点/rank/可见设备顺序。
4. 用户查询 GS 记录，验证同节点两类关联、跨任务不同 GPU 归属、重复注册和退出后的元数据变化。
5. 用户验证注册后原生生成、队列、训练和后续参数同步正常，并归档命令、配置、日志及失败证据。

本功能没有实现心跳、训练期间控制并发、空卡检测/上报、调度策略、借还或动态伸缩。
用户不得把本次注册验收通过解释为资源共享、bootstrap 或物理 GPU 释放能力已经可用。
