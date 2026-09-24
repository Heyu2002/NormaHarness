# Norma Harness

[English](./README.md) | **简体中文**

Norma Harness 是一个小型 Rust 库，用于连接在同一进程内独立实现的服务。项目把这些服务称为 **Resident**。

框架只定义两条数据流：

- **RDF（Registration Data Flow，注册数据流）**负责 Resident 注册、同名审核、能力与投递端点登记，以及新 Resident 上线通知。
- **RTDF（Runtime Data Flow，运行数据流）**负责把一条消息从已注册的源 Resident 传递到已注册的目标 Resident。

Resident 自己控制执行模型、业务状态、并发、依赖、重试、取消、回退、回滚和安全退出边界。Norma Harness 只提供注册与单跳消息传递。

## 架构

```mermaid
flowchart LR
    App["应用"] --> Harness["NormaHarness"]

    Harness --> RDF["RDF<br/>注册权威"]
    Harness --> RTDF["RTDF<br/>消息传递"]

    RDF --> Directory["注册目录<br/>名称 · 能力 · 投递端点"]
    RDF --> Store["ResidentStore<br/>实例所有权"]
    Store --> Resident["Resident 实例"]

    RTDF -->|解析路由| RDF

    Resident -. "RegistrationSender<br/>仅通道句柄" .-> RDF
    Resident -. "MessageSender<br/>仅通道句柄" .-> RTDF
```

`NormaHarness` 是组合根。它启动 RDF 与 RTDF，并向应用提供两个可克隆的通道句柄，由应用在构造 Resident 时注入。句柄不持有 RDF、RTDF、ResidentStore 或整个应用对象。

| 组件 | 职责 |
| --- | --- |
| `Resident` | 实现一个独立服务，对外暴露名称、能力、邮箱和可选 Gate。 |
| `NormaHarness` | 创建并持有框架数据流，提供最小注入入口。 |
| `RDF` | 串行处理注册变更，审核同名，维护公开注册目录，广播新 Resident。 |
| `ResidentStore` | 按实例 ID 强持有注册成功的 Resident，是框架内部存储。 |
| `RTDF` | 解析一个目标并完成单跳投递，不执行业务逻辑。 |
| `Gate` | Resident 自己拥有的传输边界，可以校验或转换消息。 |
| `Mailbox` | 将 `ResidentEvent` 入队，交给 Resident 稍后处理。 |

整个模型依赖四条核心约束：

1. 同一个 Resident 名称最多对应一个存活的注册。
2. 每次成功注册都会得到一个进程内不可复用的 `ResidentInstanceId`。
3. 消息固定经过源 Resident 的传出 Gate、目标 Resident 的传入 Gate 和目标邮箱。
4. 业务状态与业务生命周期始终留在具体 Resident 内部。

## 添加依赖

通过 Git 仓库引用：

```toml
[dependencies]
norma-harness = { git = "https://github.com/Heyu2002/NormaHarness" }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

`NormaHarness::new()` 会启动后台任务，因此必须在 Tokio runtime 内调用。

## 最小示例

下面的示例定义了一个持有两个注入端口的 Resident。它在准备完成后主动注册自己，发送一条消息，并在自己的工作完全停止后执行注销。

```rust
use std::{error::Error, sync::Arc};

use norma_harness::{
    CapabilityKey, FlowMessage, IdentifierError, Mailbox, MailboxAddress,
    MailboxError, MessageKind, MessageSender, NormaHarness, RegistrationError,
    RegistrationReceipt, RegistrationSender, Resident, ResidentDescriptor,
    ResidentEvent, ResidentInstanceId, ResidentKey, RouteError,
};
use serde_json::json;

struct PrintMailbox;

impl Mailbox for PrintMailbox {
    fn deliver(&self, event: ResidentEvent) -> Result<(), MailboxError> {
        // 真实邮箱应当只完成入队，然后立即返回。
        println!("{event:?}");
        Ok(())
    }
}

struct Service {
    descriptor: ResidentDescriptor,
    mailbox: MailboxAddress,
    registration: RegistrationSender,
    messages: MessageSender,
}

impl Service {
    fn new(
        name: &str,
        capability: &str,
        registration: RegistrationSender,
        messages: MessageSender,
    ) -> Result<Self, IdentifierError> {
        Ok(Self {
            descriptor: ResidentDescriptor::new(
                ResidentKey::new(name)?,
                [CapabilityKey::new(capability)?],
            ),
            mailbox: Arc::new(PrintMailbox),
            registration,
            messages,
        })
    }

    async fn register(
        self: &Arc<Self>,
    ) -> Result<RegistrationReceipt, RegistrationError> {
        self.registration.register(Arc::clone(self)).await
    }

    async fn send(
        &self,
        instance_id: ResidentInstanceId,
        target: &ResidentKey,
    ) -> Result<(), RouteError> {
        self.messages
            .send(
                instance_id,
                target,
                FlowMessage::new(
                    MessageKind::new("request").expect("静态消息类型合法"),
                    json!({ "prompt": "store this value" }),
                ),
            )
            .await
    }

    async fn unregister(
        &self,
        instance_id: ResidentInstanceId,
    ) -> Result<(), RegistrationError> {
        self.registration.unregister(instance_id).await.map(|_| ())
    }
}

impl Resident for Service {
    fn descriptor(&self) -> ResidentDescriptor {
        self.descriptor.clone()
    }

    fn mailbox(&self) -> MailboxAddress {
        self.mailbox.clone()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let harness = NormaHarness::new();
    let registration = harness.registration_sender();
    let messages = harness.message_sender();

    let llm = Arc::new(Service::new(
        "llm",
        "generate",
        registration.clone(),
        messages.clone(),
    )?);
    let storage = Arc::new(Service::new(
        "storage",
        "store",
        registration,
        messages,
    )?);

    let llm_receipt = llm.register().await?;
    let storage_receipt = storage.register().await?;
    let storage_key = storage.descriptor().key().clone();

    llm.send(llm_receipt.instance_id(), &storage_key).await?;

    // 每个 Resident 先到达自己的安全退出边界。
    llm.unregister(llm_receipt.instance_id()).await?;
    storage
        .unregister(storage_receipt.instance_id())
        .await?;

    Ok(())
}
```

## 注册与消息投递时序

```mermaid
sequenceDiagram
    autonumber
    participant App as 应用
    participant Source as 源 Resident
    participant Reg as RegistrationSender
    participant RDF
    participant Store as ResidentStore
    participant Msg as MessageSender
    participant RTDF
    participant Target as 目标 Resident

    App->>Source: 构造并注入 registration、messages
    Source->>Reg: register(Arc<Self>)
    Reg->>RDF: 注册命令
    RDF->>Source: 读取描述、邮箱和 Gates
    RDF->>RDF: 校验实例与名称唯一性
    RDF->>Store: 按 InstanceId 持有实例
    RDF->>RDF: 提交注册记录
    RDF->>Target: ResidentRegistered(Source)
    RDF-->>Source: 回执与现有 Resident 快照

    Source->>Msg: send(源 InstanceId, 目标名称, 消息)
    Msg->>RTDF: 投递命令
    RTDF->>RDF: 解析源与目标
    RDF-->>RTDF: 源 Gate、目标 Gate、目标邮箱
    RTDF->>Source: 传出 Gate
    Source-->>RTDF: 接受或转换后的消息
    RTDF->>Target: 传入 Gate
    Target-->>RTDF: 接受或转换后的消息
    RTDF->>Target: 入队 Message 事件
    RTDF-->>Source: 投递结果

    Source->>Source: 停止新工作并处理已有工作
    Source->>Reg: unregister(InstanceId)
    Reg->>RDF: 注销命令
    RDF->>RDF: 删除注册记录
    RDF->>Store: 释放实例强引用
    RDF-->>Source: 注销结果
```

## 注册约定

Resident 应在所有公开端点准备完成后注册。注册请求携带真实的 `Arc<dyn Resident>`，RDF 直接从实例读取描述、邮箱和 Gate，不接受调用方另外拼装一份可能失真的注册记录。

成功返回的 `RegistrationReceipt` 包含：

- 本次注册分配的 `ResidentInstanceId`；
- 注册前已经存在的 Resident 快照；
- 向现有 Resident 广播新注册事件时发生的通知失败。

RDF 是唯一的同名审核点。并发注册同一个名称时，注册命令会被串行处理，最终只能有一个实例成功。广播通知失败会记录在回执中，不会回滚新注册，也不会删除邮箱拒绝通知的 Resident。

从注册成功到注销完成，Resident 的描述和投递端点必须保持稳定。需要变更公开身份或端点时，应先让旧注册安全退出，再注册一个新实例。

## 消息传递约定

一次发送只包含三个核心值：

```text
源 ResidentInstanceId
目标 ResidentKey
FlowMessage { 消息类型, JSON 载荷 }
```

RTDF 只执行一次单跳投递：

```text
RDF 解析路由
→ 源 Resident 传出 Gate
→ 目标 Resident 传入 Gate
→ 目标 Resident 邮箱
```

源 Resident 必须提供本次注册对应的准确实例 ID。旧实例注销后，即使新实例复用了相同名称，旧 ID 也不会重新生效。目标 Resident 按当前唯一名称查找。

两个 Gate 都可以接受、拒绝或转换消息。只有两个 Gate 都成功后，目标邮箱才会收到 `ResidentEvent::Message(RoutedMessage)`。Gate 或邮箱失败只终止本次投递并返回 `RouteError`，不会改变任何 Resident 的注册状态。

`Mailbox::deliver` 是同步接口，只应该完成入队。具体业务处理应当在 Resident 自己的任务或执行器中进行。

## 生命周期与并发

注销是 Resident 最后一次框架操作：

```text
停止接收新工作
→ 排空或取消 Resident 自己拥有的工作
→ 关闭或保护公开端点
→ 注销准确的 InstanceId
→ Resident 执行返回
```

RDF 在注销时不会等待 Resident 的执行函数结束，否则容易形成双方相互等待的生命周期死锁。Store 删除强引用也不等于强制销毁 Rust 对象：其它 `Arc` 仍可能让旧对象存活，但它已经失去 RDF 注册身份，无法再用旧实例 ID 发起新的 RTDF 消息。

已经从 RDF 复制了 Gate 和邮箱句柄的投递可能与注销并发结束。具体 Resident 必须在自己的退出边界内保证迟到调用能够安全失败或安全完成。

RDF 串行处理注册命令。RTDF 独立调度每次投递，因此 Gate 可以等待异步操作，也可以再次发起消息，不会阻塞一个全局串行投递循环。即使外部仍保留 Sender 克隆，释放 `NormaHarness` 也会关闭框架核心。

## Resident 实现包

[`norma-residents`](./residents/README.md) 包统一存放具体
Resident 实现。当前的 `codex` 模块将本机已登录的 Codex CLI 接为 Resident，
通过 stdio 使用
`codex app-server`，默认模型为 `gpt-6-luna`，并经 RTDF 返回结果。Codex
线程状态、执行和退出都由这个具体 Resident 负责，不加入 RDF 或 RTDF。

```console
cargo run -p norma-residents --example roundtrip -- . "用一句话概括这个仓库。"
```

## Resident 聊天室

本仓库提供本地网站 `norma-web`。网页从 RDF 查询带 `llm` 能力的 Resident，
在左栏直接显示在线模型。点击模型会打开或复用它的单聊房间；创建群聊时才选择
至少两名不同的在线 Resident。群聊未使用 `@成员` 时，全部成员并行回复；
使用 `@成员` 或 `@{成员}` 时，先由被提及的群成员回复。模型可 `@其他成员` 继续讨论，
认为答案完整时以 `@你` 开头直接回答用户。同一个 Resident 在私聊和群聊中
为每个房间维护独立的模型线程，并接收它参与的普通房间消息；无痕房间只接收本房间上下文。
所有请求与结果都经过 RTDF，房间协调状态由 `residents::chat` Resident 持有。
聊天室在 `llm.turn.request` 中提供 `origin`：`kind` 为 `solo` 或 `group`，
并带房间 ID 和名称。LLM 入站 Gate 校验来源，并使用 RTDF 的真实发送者填充
`source_resident`。Codex 向模型发送一条简短的用户通知，说明消息来自哪个房间、
本轮是独立回复还是被其他成员提及；模型通过 `read_chat_context` 读取消息正文和历史。独立的
`tools.rooms` Resident 通过 RTDF 提供工具目录，Codex 将 `read_chat_context`、
`list_group_members`、`read_resident_memory` 和 `publish_media` 注册为 app-server 动态工具。
模型调用 `read_chat_context` 时，工具 Resident 经 RTDF 向 `chat.rooms` 查询消息；
结果只包含调用者参与的房间，并受本轮消息快照和无痕隔离限制。
`list_group_members` 仅允许查询调用者所在的当前群聊。动态工具仍属 Codex app-server 实验性协议。

输入框可选择、粘贴或拖入 PNG、JPEG、WebP 和 GIF，每条消息最多 4 个文件，
单个文件最多 8 MiB。原始 GIF 在网页中播放并可下载；传给 Codex 的是首帧图片。
Codex 生成的图片可通过 `publish_media` 动态工具交给服务 Resident 发布为聊天附件，
app-server 的 `imageGeneration` 图片结果也会进入附件。只回复文件名或路径不会显示图片。

先使用 `codex login` 登录本机 Codex CLI，然后在仓库根目录运行：

```console
cargo run -p norma-web
```

本机打开 <http://127.0.0.1:3000>；同一局域网可使用运行电脑的 IPv4 地址与端口访问，
例如 `http://192.168.2.125:3000`。网站启动两个 Codex Resident：
`codex` 使用原有 `CodexResident` 和 `gpt-6-luna`，`codex-5.6-luna` 使用独立的
`Codex56LunaResident` 和 `gpt-5.6-luna`。两者各有独立的
app-server 进程和模型线程映射，默认沿用本机 Codex 推理强度，默认只读。
可用 `NORMA_CODEX_CWD` 指定工作目录，`NORMA_WEB_BIND` 指定监听地址，
`NORMA_CODEX_EFFORT` 调整推理强度，
`NORMA_CODEX_WORKSPACE_WRITE=1` 允许修改工作目录。网站默认监听所有 IPv4 网卡；
若只允许本机访问，设置 `NORMA_WEB_BIND=127.0.0.1:3000`。目前没有用户认证，
仅应在可信网络开放。普通房间、消息、图片附件和模型线程映射自动保存在本机，
默认目录为 Windows 的 `%LOCALAPPDATA%\NormaHarness`（其他系统使用 XDG/HOME 数据目录），
可用 `NORMA_DATA_DIR` 修改。每个在线模型在侧栏只有一个入口；“创建群聊”下方的“无痕模式”选择框统一决定新开的单聊和群聊是否无痕，浏览器会记住该选择，已有会话保留原模式。归档聊天不在聊天侧栏显示；“设置”弹窗可搜索、筛选归档聊天并逐项取消归档，恢复后直接打开会话。关闭无痕后点击模型入口可继续原有痕单聊；
关闭页面或长时间没有对话会让房间休眠，下一段对话仍使用原房间 ID。
`NORMA_THREAD_IDLE_SECS` 默认 1800 秒。若旧模型线程无法恢复，Resident 会新建线程并要求模型通过 `read_chat_context` 读取本地历史。
执行中的对话会等待 Resident 自行报告完成或失败，Norma 不设置单轮执行时限。
页面将 Resident 回复按 Markdown 显示，包括表格、链接和代码块；本地历史仍保存原始文本。
“长期记忆”默认关闭，可在左下角“设置”中开启；开启后仅在房间休眠（先执行可选的结束钩子）或模型上下文压缩时提取，
模型需要时通过 `read_resident_memory` 读取长期记忆。
原始事实写入本地文件，短时间重复提及进入 cache，持续存在后进入 hot；hot 一周不提及会退行。
无痕房间不写入 Norma 的聊天快照和长期记忆；附件使用临时目录，但模型提供方及操作系统仍可能保留处理记录。
目前没有用户认证，因此记忆开关属于当前本地服务实例，不是独立的多用户设置。只有一个在线模型时，
可以单聊；当前两个 Codex Resident 均在线时可以建群。

其他实现要参与聊天室，需声明 `llm` 能力，并实现
[`residents/src/llm.rs`](./residents/src/llm.rs) 中的
`llm.turn.request` / `llm.turn.result` 协议。Codex Resident 同时兼容原有的
`codex.turn.*` 消息。

## 仓库结构

```text
src/
├── application.rs     # 组合根与注入端口
├── rdf.rs             # 注册命令与注册目录
├── resident_store.rs  # 注册实例的强所有权
├── rtdf.rs            # 单跳消息管道
├── resident.rs        # Resident 接口与注册事件
├── gate.rs            # 传入与传出消息边界
├── mailbox.rs         # Resident 自己拥有的投递地址
├── message.rs         # 原始消息与路由消息
├── id.rs              # 名称、能力键与实例 ID
└── error.rs           # 边界错误
```

更完整的不变量与模块边界见[`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md)。

## 开发

```console
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

## 许可证

Apache-2.0。
