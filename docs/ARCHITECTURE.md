# Village Harness 架构

本文记录当前 Rust 实现已经确定的架构语义。它是实现约束，不是对未来产品模块的预先拆分。

## 1. 村庄、Resident 与 Layout

Village Harness 将每个 AgentThread 视为一座独立村庄，系统模块是可插拔的 Resident，Gate 是数据路径上的关卡。Resident 之间不得直接持有彼此的实现引用；所有运行数据均通过 Layout 传输。

Layout 包含两个数据流容器：

- **RDF（Registration Data Flow）**：接受 WASM Resident artifact、受信任 Native Resident registration 与 Gate Factory 的成组注册请求，管理暴露信息，在用户 Turn 开始时完成发现并发布不可变注册快照。
- **RTDF（Runtime Data Flow）**：为每个 AgentThread 建立独立 Resident 实例、私有信箱与 Gate 实例集，串行执行同一 Thread 的 Turn，传输 Resident 产生的数据，执行 Gate，并提交状态迁移。

RDF 保存经过验证和编译的 WASM 模块或 Native Resident Factory，不保存供所有对话共享的 Resident 实例。RTDF 中的 WASM Store、Native Resident 与 Gate 实例都属于单个 AgentThread。

RDF 是 Layout 控制面，不作为 capability 注入 Resident。内部 `RegistrationSnapshot` 只交给 RTDF；RTDF 从当前固定快照生成 `ResidentDirectorySnapshot` 纯数据投影，随 `ResidentInvocation` 交给 Resident。

## 2. 注册、发现与实例化

WASM Resident 以 `ResidentArtifact` 暴露自身，其中只有 `ResidentProfile`、WASM bytes 和 `ResidentLimits`，不含活的 Rust 对象、闭包、Store 或其他 Resident 引用。RDF 编译模块并拒绝所有 import。

受信任的进程内业务可以实现公开的 Rust `Resident` trait，并通过 `NativeResidentRegistration` 提交 Factory。Layout 为每个 AgentThread 单独调用 Factory；RDF 不保存或共享 Factory 产生的实例。Native `ResidentContext` 只有本次调用的数据以及暂存 `Emission` / `StateEvent` 的方法，不包含 RDF、RTDF、mailbox 或其他 Resident 实例。Gate 是受信任的原生内核扩展，仍使用 `GateFactory`。

一次相关变更通过同一个 `RegistrationBatch` 提交。RDF 为每个 Upsert 分配新的 `ExposureId`；它表示一次暴露事件，不是业务版本号。

每个用户 Turn 开始时，RTDF 调用 RDF 执行发现：

1. RDF 依次处理待发现的注册批次。
2. 同一批次内的 Resident 与 Gate 变更全部通过校验后，才产生新的不可变 `RegistrationSnapshot`。
3. 每个 AgentThread 在自己的 Turn 边界，根据最新快照为变化的 Resident 创建独立 WASM Store 或 Native Resident 实例与私有信箱，并创建变化的 Gate 候选实例。
4. 全部候选实例初始化成功后，Thread 的 RuntimeSet 才原子切换。
5. 任意候选初始化失败时，整批候选被关闭，Thread 继续使用完整的旧 RuntimeSet。

当前正在运行的 Turn 固定使用启动时取得的 RuntimeSet。热更新不会改变执行到一半的数据流；下一 Turn 才会尝试应用新快照。

## 3. AgentThread 生命周期

同一 AgentThread 的 Turn 严格串行。不同 AgentThread 拥有不同的 Resident、Gate 和 `AgentThreadContext` 实例，可以并发运行。

Thread 长时间空闲后可以进入硬休眠：

- 等待当前 Turn 完成；
- 关闭 Resident/Gate 实例；
- 移除 AgentThreadContext；
- 释放 RTDF 中的 Thread 连接。

相同 Thread 标识再次进入时会按 RDF 最新快照重新建立 Context 和所有运行实例。当前内核不承诺跨硬休眠恢复运行状态；需要恢复时，应由未来的恢复协议显式提供数据，而不是让 Layout 无限保留内存对象。

## 4. 数据流与路径决定权

RTDF 将 `FlowPacket` 和当前 Context 的数据副本组成 `ResidentInvocation`，通过私有信箱投递。WASM Driver 使用 `ResidentResponse` ABI 解码输出；Native Driver 创建受限 `ResidentContext` 并调用用户的 `Resident::handle`。两条路径最终都只能向 RTDF 交回 `ResidentEffect`，其中可以包含：

- 零个或多个 `Emission`；
- 零个或多个 `StateEvent`。

每个 Emission 由 Resident 自己指定目标 Resident ID，或返回调用方。ID 只是地址，不是实体引用。RTDF 不替 Resident 决定下一跳，因此允许前进、返回、自循环和跨 Resident 循环。RTDF 为新包生成 message ID、保留 correlation 并推进 hop；框架不维护或验证发送者身份。

Resident 通过 `ResidentContextSnapshot.directory` 获知当前快照内的居民。目录条目只包含：

- `ResidentKey` 门牌；
- 接受的 `MessageKind`；
- `provided_capabilities` 公开能力。

Resident 可以按门牌查找，也可以使用 `providers(capability)` 按能力发现目标。目录的 `snapshot_id` 与本 Turn 固定的 RuntimeSet 一致。内部 `ExposureId`、`CompiledResident`、WASM Store、`ResidentMessenger`、Gate 和 Factory 不属于目录协议，不能被投影或序列化给 Resident。

为了让错误实现不会无限占用执行器，Turn 使用可配置的报文数和单报文 Hop 上限。这些限制约束运行资源，不定义合法业务路径。

## 5. Gate

Resident 生命周期先于 Gate 独立定义。一次有效投递固定经过：

```text
BeforeReceive
    → Execute
    → AfterExecute
    → BeforeCommit
    → Commit
```

消息契约不接受、Resident 执行失败或状态提交失败时进入 `OnFailure`，并通过 `ResidentFailureStage` 标明 `BeforeReceive`、`Execute` 或 `Commit`。Gate 只是能够挂载到这些 Hook 的一种受信任组件，生命周期本身不依赖 Gate。

Gate 与 Resident 使用同一 RDF 注册和 AgentThread 激活时机。每个 `GateProfile` 必须显式提供一个或多个 `GateHookBinding`：

```text
(ResidentKey, ResidentHookPoint)
```

RTDF 在激活时把 Gate 编译为按居民和 Hook 点索引的 chain。进入 B 只运行 B 的 `BeforeReceive` chain；A 执行完成只运行 A 的 `AfterExecute` 和 `BeforeCommit` chain。Gate 不再全局遍历，也不能看到未挂载居民的数据。

Gate 可以清洗或丢弃数据，也可以处理异常并重定向数据流。Gate 实例同样属于单个 AgentThread，不在对话间共享可变状态。

`BeforeReceive` 只能继续并改写消息、丢弃或显式重定向；correlation 和 hop 仍由 RTDF 控制。`AfterExecute` 与 `BeforeCommit` 接收完整 `ResidentEffect`，可以原子检查 Emission 和 StateEvent。`OnFailure` 可以传播、丢弃或重定向失败。

RDF 拒绝没有任何 Hook 的 Gate，也拒绝挂载到不存在 Resident 的 Gate。删除 Resident 时，相关 Gate 必须在同一注册批次中删除或重新挂载。

## 6. ToolResident

`ToolResident` 是一个受信任的 Native Resident，而不是 RDF 中的第二套注册系统。用户通过 `ToolResident::builder` 配置 Tool Factory；每个 AgentThread 激活时都会得到自己的 ToolResident 与 Tool 实例。

RDF 的类型系统、索引与校验逻辑只认识 ToolResident 的 `ResidentProfile`，其中声明 `tools.list`、`tools.invoke` 与 `tools.cancel` 三类居民级消息；它没有单 Tool 的类型、索引或注册入口。Tool 定义会被封装在不透明的 Native Resident Factory 中并由注册快照保活，但 RDF 无法枚举或解释它们。单个 `ToolKey`、Tool schema、Native handler 或 MCP client 都不会被投影到 `ResidentDirectorySnapshot` 或进入 Gate 索引。动态 MCP 发现也应更新 ToolResident 自己的 Catalog，而不是修改 RDF。

调用方通过 RTDF 发送纯数据请求。由于框架不维护消息发送者身份，请求显式携带 `reply_to: FlowTarget`；它是业务回信地址，不是来源证明。ToolResident 返回 `tools.catalog` 或带 `call_id` 的 `tools.result`。

Gate 仍只按 `(ResidentKey, ResidentHookPoint)` 挂载，因此挂到 ToolResident 的 Gate 自然覆盖其全部 list/invoke/cancel 消息。内核不提供单 Tool Gate。参数清洗、工具特定校验和业务错误属于 `Tool::invoke`；工具业务错误编码成正常 `ToolResult::Error`，不进入 Resident `OnFailure`。

普通 Resident 没有强制 Tool 字段或 Tool 注册接口，可以在不存在任何 ToolResident 时独立运行。只有需要共享工具的业务或 AgentLoop 才保存 ToolResident 的 `ResidentKey` 并通过 RTDF 发消息。

当前 mailbox 串行执行一次 Resident delivery。`tools.cancel` 只为管理后台工作的 Tool 提供 best-effort hook，不能中断正在占用同一 ToolResident mailbox 的同步 `invoke`。真正的抢占式取消需要未来的后台完成事件与 RTDF 唤醒协议，不能由当前接口虚假承诺。

当前 Native Driver 也没有内核级执行超时、panic containment 或自动重启 supervisor。一个永不返回的 Native Resident/Tool 会阻塞它的 mailbox；由于同一 AgentThread 的 Turn 串行，还会阻止该 Thread 的后续 Turn 与硬休眠。Native 业务必须自行设置下游超时并避免 panic；要把这项责任收回内核，需要后续增加明确的 deadline、取消传播与实例重建策略。

## 7. 状态机

状态机定义由 `ResidentProfile` 提供，经 RDF 统一注册和校验。实际状态实例内化在 `AgentThreadContext` 中。

Resident 不直接写入状态，也不指定最终状态。它只产生 `StateEvent`；RTDF 根据已注册定义计算迁移，并在全部事件均有效时原子提交。Resident 只能产生自己拥有的状态机事件。

热更新时，新的状态机定义必须接受 AgentThreadContext 中的当前状态。若不能接受，该 Thread 的整批热更新失败并继续使用旧 RuntimeSet。当前内核不提供通用回退语义；需要回流或引导的 Resident 应在自己的状态机定义中明确表达事件和迁移。

## 8. 隔离保证与信任边界

WASM Resident 执行路径同时使用以下机制：

1. 协议 crate 不公开宿主对象；WASM 注册入口只接受数据型 `ResidentArtifact`。
2. 每个 Resident 实例由独立 `wasmi::Store` 承载；Store 只被一个私有 mailbox task 持有，RTDF 只有 `ResidentMessenger` 的 crate-private sender。
3. Resident 模块必须零 import。宿主不链接 WASI、文件、网络、时钟、Layout callback 或 peer lookup 能力。
4. 输入和输出只能是序列化的 `ResidentInvocation` / `ResidentResponse`；Context 和 Directory 都是一次性数据快照，不是 capability。
5. 内存、输入、输出、fuel 和 mailbox 容量均受限。硬休眠或热替换会关闭信箱并丢弃 Store。

因此，在 Village Harness 的 WASM 执行路径内，Resident 没有可用于获得或保存另一个 Resident 实体的能力。两个 Resident 之间唯一受支持的通信是“返回 Emission → RTDF 排队 → 目标 mailbox 接收”。

Native Resident 是为了框架内置模块和可信业务代码提供的易用路径。框架不向 `Resident::handle` 注入 peer instance、RDF、RTDF 或 mailbox，并保证每个 AgentThread 由 Factory 创建独立实例；但同进程 Rust 代码仍可能自行使用全局变量、网络或捕获的宿主对象。因此 Native 路径是 API capability boundary，不是针对恶意代码的安全沙箱。需要敌对代码隔离时必须使用零 import WASM 或未来的独立进程 Driver。

这项保证不把受信任的原生 Gate、宿主应用本身或未来显式引入的 sidecar 算作不可信 Resident。若将来向 WASM 链接文件、网络、共享内存或通用 host-call，必须将其视为 capability，并重新审计隔离边界；不能仍宣称零通道保证。

## 9. Crate 边界

`village-harness-protocol` 只包含稳定契约：标识符、数据包、Resident 的纯数据调用/响应、Gate 与状态机。它不依赖 WASM Store、RTDF 的内部存储或调度实现。

`village-harness` 实现公开的 Native `Resident` 业务接口、内置 ToolResident、Resident artifact 编译、WASM 隔离宿主、私有信箱、RDF、RTDF、AgentThreadContext、成组注册事务、按 Thread 激活、数据传输和硬休眠。

Native Resident 与 Gate 使用受信任 Rust trait；WASM Resident 的跨编译产物边界由版本化 ABI 定义，详见 `RESIDENT_ABI.md`。未来若增加进程隔离载体，应保持相同的 capability 边界，不得重新暴露原生 Resident 实例。
