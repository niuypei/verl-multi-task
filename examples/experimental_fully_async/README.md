# 原生入口配置

状态：入口已接线；此处只提供 overrides，不复制 Hydra 主配置。

用户仍使用 `python -m verl.experimental.fully_async_policy.fully_async_main`，
并增加 `multitask.runtime.profile=experimental_fully_async_standalone`。
字段缺失或为 null 时，用户继续使用原生类型。

[native_entry_overrides.txt](native_entry_overrides.txt) 只列接入和原生前提参数，不是完整可运行的模型/数据配置。
用户必须补充原生模型、数据路径、trainer/rollout 节点和 GPU 参数，且资源必须容纳至少一个 replica。
当前 profile 使用 vLLM 非 PD、纯 STANDALONE；profile 不验证所有原生放置和并行约束。

GS name/namespace 由子仓固定；单一 profile 不包含逐类 FQN。
真实环境的所有节点必须具备相同的源码搜索路径和 verl/vLLM 依赖。
