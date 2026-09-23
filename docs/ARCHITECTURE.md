# Norma Harness 架构边界

Norma Harness 只提供两个数据流：RDF 处理注册，RTDF 处理 Resident 之间的一跳消息。Resident 自己控制业务状态和生命周期。

```text
NormaHarness
├── RDF
│   ├── ResidentStore
│   └── RegistrationDirectory
└── RTDF ── reads ──▶ RegistrationDirectory
```

`NormaHarness` 是组合根，只负责创建数据流并提供注入端口，不保存具体 Resident 实例，也不解释 Resident 的内部状态。

## 1. 所有权图

框架内部的强所有权只有以下方向：

```text
NormaHarness
├── Arc<Rdf>
│     └── ResidentStore
│           └── Arc<dyn Resident>
└── RtdfCore
      └── Arc<Rdf>
```

Resident 可以保存两个可克隆端口：

```text
Resident
├── RegistrationSender ──▶ registration channel
└── MessageSender      ──▶ route channel
```

Sender 只持有通道发送端，不持有 `Arc<Rdf>`、`Arc<Rtdf>`、`Arc<ResidentStore>` 或 `Arc<NormaHarness>`。RDF 和 RTDF 的命令循环只持有核心对象的 `Weak`，而且核心对象不保存捕获自身的 `JoinHandle`。

因此不存在以下强引用环：

```text
RDF → ResidentStore → Resident → RDF
RDF → ResidentStore → Resident → RTDF → RDF
core → JoinHandle → task → core
Resident → JoinHandle → task → Resident
```

具体 Resident 不得自行把 RDF、RTDF、ResidentStore 或整个 `NormaHarness` 包装进 `Arc` 后保存。框架提供 Sender，就是为了避免注入这些核心对象。

## 2. 模块依赖方向

```text
id / message / mailbox / gate
               ▲
               │
            resident
               ▲
               │
        resident_store
               ▲
               │
              rdf ◀── rtdf
               ▲       ▲
               └── application
```

`Resident` trait 不引用 RDF、RTDF 或应用层。RDF 只依赖抽象的 `dyn Resident`，不依赖任何具体 Resident 实现。具体实现通过公开的 Sender 与数据流通信，因此不会产生模块反向依赖。

## 3. 最小 Resident 接口

框架只读取注册和路由所需的公开端点：

```rust
pub trait Resident: Send + Sync + 'static {
    fn descriptor(&self) -> ResidentDescriptor;
    fn mailbox(&self) -> MailboxAddress;
    fn outbound_gate(&self) -> Option<Arc<dyn Gate>>;
    fn inbound_gate(&self) -> Option<Arc<dyn Gate>>;
}
```

接口不包含 `start`、`stop`、`retry`、`rollback`、状态机、快照或更新方法。具体 Resident 可以采用任何内部执行模型。

`ResidentDescriptor`、邮箱和 Gate 从注册成功到注销完成必须代表同一个公开身份。若这些端点需要变化，Resident 应完成旧实例的退出，再注册新实例。

## 4. RDF：注册与实例所有权

应用在构造具体 Resident 时注入 `RegistrationSender`。Resident 的公开端点准备完成后主动提交真实实例：

```rust
let receipt = registration_sender.register(self_arc).await?;
```

注册命令携带 `Arc<dyn Resident>`，不是调用方拼装的描述副本。RDF 按以下顺序执行：

1. 从实例读取描述、邮箱、传出 Gate 和传入 Gate；
2. 判断该实例是否已经注册；
3. 分配进程内不可复用的 `ResidentInstanceId`；
4. 在唯一注册目录中检查 Resident 名称；
5. 将实例写入私有 ResidentStore；
6. 将名称、实例 ID、能力、邮箱和 Gate 写入注册记录；
7. 释放注册目录写锁；
8. 通过已有注册记录中的邮箱广播 `ResidentRegistered`；
9. 返回实例 ID、已有 Resident 快照和通知失败列表。

Store 插入和注册记录提交位于同一个 RDF 命令中。注册失败的实例不会留在 Store。同名检查只在 RDF 进行，因此不存在 Store 与 RDF 各自判断名称的双重权威。

广播直接使用 RDF 注册记录里的 `MailboxAddress`。RDF 不需要通过 Store 找回 Resident，也不经过 RTDF，因此没有 `RDF → RTDF → RDF` 依赖。

通知失败会进入 `RegistrationReceipt::notice_failures`，不会自动注销任何 Resident。

## 5. ResidentStore：RDF 的私有所有权区

ResidentStore 只完成三件事：

1. 分配单调递增且不复用的实例 ID；
2. 按实例 ID 强持有注册成功的 `Arc<dyn Resident>`；
3. 在注销时删除该强引用。

ResidentStore 不公开给应用和 Resident，不检查名称，不保存注册状态，不路由消息，也不调用业务生命周期方法。

RDF 注册目录和 ResidentStore 维持以下不变量：

```text
存在 RegistrationRecord(instance_id)
⇔
ResidentStore 中存在相同 instance_id 的实例
```

## 6. RTDF：一跳消息传递

具体 Resident 只保存 `MessageSender`。发送请求为：

```text
send(source_instance_id, target_name, message)
```

RTDF 执行固定的一跳流程：

```text
RDF 校验源实例并解析目标名称
→ RDF 返回源传出 Gate、目标传入 Gate和目标邮箱
→ 源传出 Gate
→ 目标传入 Gate
→ 目标邮箱
```

RTDF 不访问 ResidentStore。一次发送开始时，它从 RDF 注册记录复制必要端点，随即释放目录读锁，再执行异步 Gate。Gate 或邮箱失败只终止本次发送并返回错误，不修改注册状态。

消息通道发送端不拥有 `RtdfCore`。RTDF 后台循环只持有 `Weak<RtdfCore>`，所以 Store 内 Resident 即使保存 `MessageSender`，也不会通过它返回 RTDF 或 RDF。

## 7. 实例身份与同名复用

`ResidentInstanceId` 标识一次具体注册，进程生命周期内不复用：

```text
旧实例：InstanceId 17，名称 llm
新实例：InstanceId 42，名称 llm
```

旧实例注销后，新实例可以注册相同名称。RTDF 会拒绝实例 17 继续作为发送源，因为 RDF 已删除 `by_instance[17]`。

实例 ID 不是跨进程安全凭证。它用于区分实例代次，框架仍然信任进程内 Resident。

## 8. 注销是 Resident 的最后一个框架动作

合法顺序为：

```text
应用构造 Resident 并注入两个 Sender
→ Resident 主动注册自己
→ Resident 正常处理工作
→ Resident 停止接受新工作
→ Resident 排空或取消自己拥有的工作
→ Resident 主动注销
→ RDF 删除注册记录和 Store 强引用
→ Resident 的执行函数返回
```

RDF 处理注销时不会等待 Resident 的执行函数返回。若 RDF 等待执行结束，而执行函数正在等待 `unregister` 的响应，就会形成执行等待环。

Store 删除强引用不等于强制销毁对象。调用注销的执行栈或应用自己的引用可以让对象继续存在，但它已经没有 RDF 注册身份，不能再通过旧实例 ID 发送消息。

一次已经从 RDF 复制端点的 RTDF 投递可能与注销并发结束。框架不会替 Resident 判断这个业务边界；具体 Resident 应在注销前关闭或保护邮箱和 Gate，使迟到调用安全失败或安全结束。

## 9. 初始化与后台循环

`NormaHarness::new()` 必须在 Tokio runtime 内调用。它启动两个命令循环：

- RDF 循环处理注册和注销；
- RTDF 循环接收一跳发送，并把每次投递交给独立任务，避免 Gate 内部再次发送消息时等待同一个串行循环。

串行 RDF 命令保证同名检查与提交之间没有并发竞态。命令处理完成后，循环立即释放临时升级得到的核心 `Arc`。RTDF 投递任务只在本次消息处理期间持有核心对象，RTDF 不保存其任务句柄。核心销毁时通过独立 shutdown 通知结束循环，即使外部仍误留了 Sender，也不会让后台循环永久等待。

## 10. 明确删除和禁止回流的能力

当前架构不包含：

- Session、Thread、Turn 和上下文压缩；
- Runtime、Resident Host、Factory 和 WASM ABI；
- 版本化 Registry、热更新激活和历史回退；
- 通用状态机、检查点、快照和状态持久化；
- Ledger、Journal、恢复协调和 durable execution；
- 框架级重试、取消、补偿或回滚；
- 多阶段 Gate 生命周期和业务 Effect。

需要理解业务状态的能力属于具体 Resident，不得放入 ResidentStore、RDF 或 RTDF。

## 11. 代码结构

```text
src/
├── application.rs     # 组合根与两个注入端口
├── resident_store.rs  # RDF 私有实例所有权
├── resident.rs        # 最小 Resident 接口和注册事件
├── rdf.rs             # 注册命令、目录、广播和 RegistrationSender
├── rtdf.rs            # 一跳消息管道和 MessageSender
├── gate.rs            # 传入与传出边界
├── mailbox.rs         # 入队地址
├── message.rs         # 传输消息
├── id.rs              # 名称和实例 ID
└── error.rs           # 边界错误
```

## 12. 框架契约与具体 Resident 实现

工作区把两类代码分成两个包：

```text
Cargo.toml                       # 仓库根目录就是 norma-harness 包
src/                             # 框架契约、RDF、RTDF、ResidentStore
tests/                           # 框架测试
residents/                       # 具体 Resident 实现包
├── src/codex/                   # Codex Resident
├── src/chat/                    # 房间协调 Resident
├── src/tools/                   # 工具服务 Resident
└── src/llm.rs                   # LLM 消息契约与入站 Gate
```

框架中已有的 Resident 共用信息包括：

| 信息 | 所在位置与权威 |
| --- | --- |
| 名称和能力 | `ResidentDescriptor`；RDF 保存注册期内的公开快照。 |
| 本次注册的实例身份 | RDF 分配 `ResidentInstanceId`，通过 `RegistrationReceipt` 返回；注销后不复用。 |
| 注册通知与路由消息 | `ResidentEvent`、`FlowMessage` 和 `RoutedMessage`。 |
| 投递端点 | `Mailbox` 与可选的传入、传出 `Gate`；由具体 Resident 提供，RDF 保存注册期内的端点。 |
| 通信入口 | `RegistrationSender` 与 `MessageSender`；应用构造具体 Resident 时注入。 |

模型配置、Codex 子进程、线程 ID、队列、重试与安全退出等属于具体实现，留在
`norma-residents::codex`。框架不会给 Resident 设置统一执行基类，也不会替它保存
业务状态。若多个具体实现出现稳定的重复代码，可以先在 `norma-residents` 内抽取
共用模块，再判断是否有真正需要进入框架的通用契约。
