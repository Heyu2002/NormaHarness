# Village Harness

Village Harness 是一个面向模块热插拔的数据流运行内核。每个 AgentThread 构成一座“村庄”，系统模块作为 Resident 接入。Layout 持有全部 Resident 实例，业务 Resident 只处理数据上下文并产生 Emission，不能通过框架 API 取得其他 Resident 实例。

Resident 有两种执行路径：零宿主导入的 WASM Resident 提供强隔离；受信任的 Native Resident 直接实现 Rust `Resident` trait，提供类似普通应用框架的开发体验。两者使用同一 RDF 注册、RTDF 投递和 Gate 生命周期。

Layout 由两个容器组成：

- **RDF（Registration Data Flow）**：接收 WASM artifact、Native Resident registration 与 Gate Factory 的成组注册请求，在 Turn 边界发布不可变注册快照。RDF 只注册 Resident，不解释 Resident 内部业务。
- **RTDF（Runtime Data Flow）**：为每个 AgentThread 建立独立 Resident 实例和私有信箱，串行投递调用，执行 Gate 并提交状态迁移。

## 核心语义

- WASM Resident 以 `ResidentArtifact`（profile、WASM bytes、资源限制）注册；模块不允许任何 import，因此没有 WASI、文件、网络、时钟、宿主回调或其他 Resident handle。
- Native Resident 实现公开的 `Resident::handle`，通过 `NativeResidentRegistration` 注册。Layout 为每个 AgentThread 调用 Factory 创建新实例，只向它提供受限的 `ResidentContext`。Native 代码属于受信任宿主代码，不具有 WASM 的敌对代码隔离保证。
- `ResidentContext::send`、`reply` 和 `emit` 只暂存 Emission；实际数据包和投递仍由 RTDF 在 `handle` 成功返回后完成。RDF、RTDF、mailbox 和其他 Resident 实例都不进入 Context。
- Resident 返回只包含目标与消息的 `Emission`。RTDF 生成新的消息 ID，保留 correlation，并推进 hop 后完成投递；框架不维护或验证发送者身份。
- RDF 的内部 `RegistrationSnapshot` 只进入 RTDF。居民看到的是 RTDF 投影出的 `ResidentDirectorySnapshot`，其中只有门牌、接受的消息类型和公开能力，不含模块、Store、mailbox、Factory 或 Gate。
- Resident 生命周期固定为 `BeforeReceive → Execute → AfterExecute → BeforeCommit → Commit`，失败进入 `OnFailure`。这些 Hook 独立于 Gate 定义。
- Gate 是挂载到指定 `ResidentKey + ResidentHookPoint` 的受信任关卡，不再全局观察所有数据；没有 Hook 或引用不存在居民的 Gate 会被 RDF 拒绝。
- `ToolResident` 是普通 Native Resident。它在内部维护 Tool Catalog 和具体 Tool 实例，通过 `tools.list`、`tools.invoke`、`tools.cancel` 消息提供能力；RDF 和 Resident Directory 只看到整个 ToolResident，不看到单个 Tool。
- Gate 只挂到整个 ToolResident 的生命周期。单个 Tool 的参数清洗、业务校验和业务错误处理直接属于该 Tool 的实现，不存在 Tool 级 Gate。
- 普通用户 Resident 不要求注册或依赖 ToolResident；只有需要工具时才通过 RTDF 向某个 ToolResident 发送消息。
- 相关 Resident/Gate 更新组成同一注册事务；任一实例初始化失败，AgentThread 保留完整旧 RuntimeSet。
- 运行中的 Turn 固定当前实例集合；热更新在该 AgentThread 的下一 Turn 生效。
- Resident 决定数据下一跳，可前进、返回或循环；RTDF 只执行数据流。
- 状态机定义归 Resident，统一注册给 Layout；实际状态实例位于 AgentThreadContext，由 RTDF 根据 Resident 事件迁移。
- Thread 进入硬休眠时释放 Resident、Gate、Context 和运行连接，恢复时使用最新注册快照重新建立。

## 最小 Native Resident

用户只实现业务方法；RDF 注册、RTDF 投递、Gate 生命周期和状态提交仍由内核控制，不能被 Resident 覆盖：

```rust
use async_trait::async_trait;
use village_harness::{
    NativeResidentRegistration, RegistrationBatch, Resident, ResidentContext,
};
use village_harness::protocol::{
    FlowMessage, MessageKind, ResidentError, ResidentKey, ResidentProfile,
};

struct Greeter;

#[async_trait]
impl Resident for Greeter {
    async fn handle(&mut self, context: &mut ResidentContext) -> Result<(), ResidentError> {
        context.reply(FlowMessage::new(
            MessageKind::new("greet.result").unwrap(),
            context.message().payload.clone(),
        ));
        Ok(())
    }
}

fn registration_batch() -> RegistrationBatch {
    let registration = NativeResidentRegistration::new(
        ResidentProfile::new(ResidentKey::new("greeter").unwrap())
            .with_accepted_message(MessageKind::new("greet").unwrap()),
        || Greeter,
    );
    RegistrationBatch::new().upsert_native_resident(registration)
}
```

需要共享工具时，再单独构造一个 `ToolResident` 并将它作为普通 Native Resident 放进同一个 `RegistrationBatch`。业务 Resident 不实现任何工具注册接口，只向该居民的门牌发送 `ToolInvokeRequest`。

## Workspace

| Crate | 职责 |
| --- | --- |
| `village-harness-protocol` | Resident 的纯数据 ABI、Gate、数据包、标识符和状态机契约 |
| `village-harness` | Native/WASM Resident Host、内置 ToolResident、RDF、RTDF、私有信箱、热更新与 AgentThread 生命周期 |

详细约束见 [架构文档](docs/ARCHITECTURE.md) 与 [Resident ABI](docs/RESIDENT_ABI.md)。

## 校验

```shell
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

## License

Apache-2.0
