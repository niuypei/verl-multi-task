# 实体与初始化：完整接线，新增业务为空

状态：接线代码已实现；完整原生运行待真实环境验证。

原生基线：`/Users/nyp/Documents/multi_task_verl/verl`，
`v0.9.0-4-gadc7eefa` / `adc7eefa16dad75c5f7b878823d5a76eac90c7b3`，核对日期 2026-09-08。
以下原生行号使用该 HEAD；本轮入口补丁位于原生类定义之后，不改变下表原类行号。
配套入口补丁已提交为 `2625ae0b468a23d87c88fd5a3f08b5c5210415ad`（`v0.9.0-5-g2625ae0b`）；
该提交只修改 Fully Async main 与主 YAML。源码契约测试仍固定对比上面的未修改原生基线，不随当前 HEAD 移动。

## 1. 实体、所有者和创建点

表内所有 MultiTask 类均是伴生仓实现，不是 verl 原有类。Ray ActorClass 是创建描述；
创建者调用 `.remote()` 后才创建 Actor 并取得 ActorHandle。

| 实体 | 所有者和运行类型 | 子仓文件（相对 src/multi_task_scheduler） | 原生创建/复用点 |
|---|---|---|---|
| GroupScheduler | TaskRunner 发现的全局 Ray Actor；GS 保存 TaskRunner 句柄 | scheduler/group_scheduler.py、discovery.py | 新增发现，不替换原生资源调度 |
| MultiTaskFullyAsyncTaskRunner | driver 创建的 Ray Actor | integration/verl/experimental_fully_async/task_runner.py | trainer/main_ppo.py:91、93、94；继承 fully_async_main.py:46、51、186 |
| MultiTaskFullyAsyncTrainer | TaskRunner 创建/持有的 Ray Actor | 同目录 trainer.py | fully_async_main.py:138、146、155 |
| MultiTaskFullyAsyncRollouter | TaskRunner 创建的 Ray Actor，Trainer 也持有句柄 | 同目录 rollouter.py | fully_async_main.py:117、119、132、87 |
| MultiTaskLLMServerManager | Rollouter 内普通对象 | 同目录 llm_server_manager.py | fully_async_rollouter.py:819、847 |
| MultiTaskCheckpointEngineManager | Trainer 内普通对象 | checkpoint/checkpoint_engine_manager.py | fully_async_trainer.py:217、221 |
| MultiTaskGlobalRequestLoadBalancer | Manager 创建的 Ray Actor；Manager/client 持有句柄 | rollout/load_balancer.py | workers/rollout/llm_server.py:600、609 |
| MultiTaskvLLMReplica | Manager 持有的普通对象；Trainer 从 Rollouter 获取序列化副本 | rollout/replica.py | llm_server.py:502、556 |
| MultiTaskCheckpointEngineWorker | Replica 经 RayWorkerGroup 创建的 Ray Actor | checkpoint/checkpoint_engine_worker.py | workers/rollout/replica.py:217、225、228 |
| MultiTaskvLLMHttpServer | Replica 创建的 Ray Actor；LB 路由到 head server | rollout/http_server.py | vllm_async_server.py:1125、1168、1176 |

这里的 CE 是 Checkpoint Engine 参数同步组件，LB 是 Load Balancer 请求负载均衡器。
原生 FullyAsyncAgentLoopManager、FullyAsyncLLMServerClient、MessageQueue/Client、WorkerDict 和 ServerAdapter 仍按原生流程创建使用，
不为改名而添加包装。CE Worker 在 `verl/checkpoint_engine/base.py:323` 创建原生 ServerAdapter。

## 2. 谁创建谁

1. 原生 main 完成设备设置、rollout 资源字段映射和 reward 配置迁移，再读取 profile。
2. profile resolver 返回 MultiTaskFullyAsyncTaskRunner ActorClass。resolver 不创建 GS，不调用 ray.init 或 remote。
3. 原生 run_ppo 创建 Ray 运行环境与 TaskRunner Actor，然后调用 TaskRunner.run。
4. MultiTask TaskRunner 创建或获取真实 GS，将自己的 ActorHandle 交给 GS，并调用原生 run。
5. 原生初始化调用扩展的 _create_trainer。该方法只更换 Trainer 类型，保留角色过滤、资源池参数及 init_workers 顺序。
6. 原生初始化调用扩展的 _create_rollouter。该方法只更换 Rollouter 类型并传入 GS 句柄，随后仍执行原生 init_workers 与样本额度设置。
7. Rollouter 在原生 Manager 创建时机创建 MultiTaskLLMServerManager。Manager 使用扩展 Replica、CE Worker、HTTP Server 和 LB。
8. Manager 保留原生资源池、PG、bundle、WorkerGroup 和 GPU 绑定；LB 保留原生路由，并接收原来的 full_determinism 参数。
9. 原生 TaskRunner 将 Rollouter 句柄传给 Trainer。Trainer 从 Rollouter 取得 replicas，并创建 MultiTaskCheckpointEngineManager。
10. 原生 TaskRunner 连接 MessageQueue、恢复 checkpoint、触发初始参数同步与可选验证，随后运行原生 Trainer/Rollouter.fit。
11. TaskRunner 的 run 结束后，TaskRunner 只移除 GS 对自己的引用；清理异常不会掩盖原生训练异常。

步骤 5、6 的原生证据为 `fully_async_main.py:77—113、117—157`；步骤 7 为 `fully_async_rollouter.py:819—856`；
步骤 9 为 `fully_async_trainer.py:346、350、217—224`；步骤 10 为 `fully_async_main.py:97—113、186—219`。
Manager 位于 Rollouter，CE 位于 Trainer；TaskRunner 不能把这两个普通对象当成本地成员直接调用。

## 3. 哪些逻辑没有改变

三个 controller 的 Ray 配置分别保留 TaskRunner 1 CPU、Trainer 10 CPU、Rollouter 10 CPU/max_concurrency=100。
Replica 只覆写原有 Worker 类型选择方法和 server_class；原生 init_standalone、launch_servers、NodeAffinity/GPU 参数继续执行。
CE Manager、CE Worker 和 HTTP Server 只继承原生类；权重同步、生成与服务端构造没有新增业务覆写。
原生类定义没有修改，verl 只新增 main 的 profile 分支和主配置的可选字段。

## 4. GS 与空业务的边界

GS 发现和实体初始化是真实动作，不能使用假对象。
GS 的 task_runners 字典仅用于保留控制器引用，不保存节点/GPU、租约或 replica 资源视图。
TaskRunner 将 GS 句柄传给 Rollouter，再由 Manager 传给 LB；Trainer/CE 不持有 GS 句柄。

GS.schedule 返回空列表。代码不启动心跳或规模调整，不改变 CE 成员、LB server 集合或权重版本。
本轮删除了仅用于未来特性的通用协议、命令回执、资源登记、借用工厂、bootstrap 和 gate。
后续功能需要时再逐项增加，不用空接口伪造资源操作成功。
