# ForAGENTS：Norma Harness 开发地图

[中文项目介绍](./README.zh-CN.md) · [English overview](./README.md)

本文件用于改代码时定位职责、保持协议边界，并解释这些约束的原因。它描述当前实现；具体签名以源码为准。面向使用者的功能和价值写在中英文 README，底层所有权的完整推导见 [docs/ARCHITECTURE.md](./docs/ARCHITECTURE.md)。

## 先判断改动属于哪一层

| 需求 | 主要位置 | 对应验证 |
| --- | --- | --- |
| 注册、能力发现、实例身份 | `src/rdf.rs`、`src/resident_store.rs`、`src/resident.rs`、`src/id.rs` | `tests/data_flows.rs` |
| 消息路由、Gate、邮箱 | `src/rtdf.rs`、`src/gate.rs`、`src/mailbox.rs`、`src/message.rs` | `tests/data_flows.rs` |
| LLM 请求/结果协议与来源校验 | `residents/src/llm.rs` | `residents/tests/chat_rooms.rs` |
| 房间、群聊调度、@、归档、休眠 | `residents/src/chat/mod.rs` | `residents/tests/chat_rooms.rs` |
| 普通聊天快照 | `residents/src/chat/storage.rs` | `residents/tests/chat_rooms.rs` |
| Codex 模型进程、线程、动态工具调用 | `residents/src/codex/mod.rs`、`app_server.rs`、`protocol.rs` | Codex 单元测试、`chat_rooms.rs` |
| 工具目录、工具请求转发 | `residents/src/tools/mod.rs` | `residents/tests/chat_rooms.rs` |
| 图片/GIF 校验与存储、长期记忆 | `residents/src/media.rs`、`memory.rs` | 对应模块测试、`chat_rooms.rs` |
| HTTP、启动组合、环境变量 | `web/src/main.rs` | `cargo test -p norma-web`、HTTP 检查 |
| 页面与交互 | `web/static/index.html`、`style.css`、`app.js` | `node --check web/static/app.js`、浏览器检查 |

工作区有三个包：根目录 `norma-harness` 是通用框架，`residents/` 是具体参与者，`web/` 是把它们组装成聊天室的应用。不要因为一个聊天功能需要跨包传递消息，就把聊天状态塞进根框架。

## 核心框架的硬边界

### 1. Resident 自己拥有执行与状态

`Resident` trait 只暴露稳定的 `ResidentDescriptor`、`MailboxAddress` 和可选的传出/传入 `Gate`。它没有统一的 `start/stop`、重试器、回滚器或会话管理器。新 Resident 可以有自己的队列、子进程、任务和关闭步骤。

**原因：** 框架连接不同类型的服务，不能假定它们共享一种业务生命周期。把执行策略放进 RDF/RTDF 会让一个模型适配器的需要变成所有 Resident 的限制。

应用构造 Resident 时只注入 `RegistrationSender` 和 `MessageSender`。这两个可克隆句柄只持有通道发送端；Resident 不应保存 `NormaHarness`、RDF、RTDF 或 `ResidentStore` 的强引用。这样 `RDF → Store → Resident` 不会再经 Resident 回指框架，避免强引用环。

### 2. RDF 是唯一的注册权威

Resident 在邮箱和 Gate 准备好之后，调用 `RegistrationSender::register(Arc<Self>)`。RDF 从真实实例读取公开端点，串行审核名称唯一性，分配本进程内不复用的 `ResidentInstanceId`，并同时更新注册目录与私有 `ResidentStore`。成功回执包含实例 ID、先前的 Resident 快照和注册通知失败。通知失败只记录在回执中，不回滚已经成功的注册。

`ResidentStore` 只按实例 ID 强持有注册成功的对象；它不检查名称、不做路由，也不管理业务状态。名称及能力的查询只走 RDF 的注册目录。

**原因：** 同名审核与实例所有权必须在同一个注册命令里提交。旧实例注销后可以复用名称，但旧实例 ID 永远不能重新获得发送权。实例 ID 是进程内代次标识，不是跨进程安全凭证。

### 3. RTDF 只做一次投递

发送接口是 `send(source_instance_id, target_key, FlowMessage)`。固定顺序为：

```text
RDF 校验源并解析目标
→ 源 outbound Gate
→ 目标 inbound Gate
→ 目标 Mailbox::deliver
```

`Mailbox::deliver` 是同步接口，应当只入队，不能在里面运行模型或等待耗时业务。`send().await` 成功只表示目标邮箱接受了消息；模型结果必须用另一条 `llm.turn.result` 消息返回，并由 `request_id` 关联。

**原因：** RTDF 不理解聊天、工具或模型语义，也不负责业务重试。每次投递独立调度，Gate 内再发送消息不会被一个全局投递循环卡住。Gate/邮箱失败只结束本次投递，不改注册状态。

### 4. 注销属于 Resident 的最后阶段

顺序是停止接收新工作、排空或取消自己拥有的工作、保护公开端点，然后用准确的实例 ID 注销。RDF 不等待 Resident 的执行函数结束，否则执行函数等待 `unregister` 回执时会形成死锁。已经取到 Gate/邮箱句柄的投递可能晚于注销完成；具体 Resident 要让这种迟到调用安全完成或失败。

## 聊天与模型的实际路径

`web/src/main.rs` 在 Tokio 内创建 `NormaHarness`，然后注册 `chat.rooms`、`tools.rooms`、两个独立的 Codex Resident，再启动 Axum。当前 `codex` 使用 GPT-6 Luna；`codex-5.6-luna` 使用单独的 `Codex56LunaResident` 和 GPT-5.6 Luna。两者有各自的 app-server 进程与 provider thread 映射。原 `CodexResident` 的单实例限制仍然存在；增加另一 Resident 时不要靠解除这个限制来复用旧实例。

一次聊天轮次沿以下路径运行：

```text
网页 → HTTP → ChatResident 房间
→ RTDF: llm.turn.request → 目标 LLM Resident
→ RTDF: llm.turn.result → ChatResident
→ 房间消息 / SSE → 网页
```

协议类型在 `residents/src/llm.rs`。声明 `llm` 能力必须真的处理 `llm.turn.request` 并回送带同一 `request_id` 的 `llm.turn.result`。请求有房间 `origin` 和本轮 `context`。`LlmContextGate` 校验房间来源，并用 RTDF 的真实源 Resident 填充 `source_resident`；不要信任任意载荷里自称的来源。

`ChatResident` 拥有房间、消息、忙碌状态和响应关联。群聊无指定 @ 时，它为所有成员启动独立的初始轮次；指定 @ 时只启动被提及成员。不同成员的初始轮次并行；**同一个 Resident 跨房间的轮次仍串行**，以维持该 Resident 的上下文顺序。初始轮次结束后才处理模型之间的 @ 接力；单轮接力最多 16 次。以 `@你` 开头的完整回答会作为面向用户的总结显示。

这些调度规则放在 `residents/src/chat/mod.rs`，不放在 `web/static/app.js` 或 RTDF。网页负责呈现和发请求，不应决定哪个模型下一轮发言。

## 模型工具与上下文边界

Codex 收到的是简短的房间通知，提示先调用 `read_chat_context`；它不会把整个聊天 JSON 信封当作用户消息正文。`tools.rooms` 通过 RTDF 提供工具目录，Codex 适配器把 `read_chat_context`、`list_group_members`、`read_resident_memory` 和 `publish_media` 注册为 app-server `dynamicTools`。这是当前 Codex app-server 的实验性接口；改协议时核对 `residents/src/codex/app_server.rs` 与实际响应。

新增模型工具至少检查四处：`tools/mod.rs` 的目录与消息种类、`codex/app_server.rs` 的调用分派、拥有真实数据的 Resident、以及跨 Resident 测试。工具服务可以转发查询，但不能凭模型传入的房间 ID 自行授予访问权限。

`read_chat_context` 的可见范围来自当前轮次快照。`ChatResident` 校验调用者确实是房间成员、消息 ID 未超过本轮可见上界，并隔离无痕房间；普通房间可按需读取该模型参加的其他普通房间，无痕只可读取本房间。保持这些检查在真实数据所有者处，避免模型从工具参数扩大权限。

`publish_media` 把真实图片字节交给媒体服务。仅在模型文本里写文件名不会生成附件。图片上限为每条消息 4 个、每个 8 MiB；支持 PNG、JPEG、WebP 和 GIF。普通媒体持久化，无痕媒体放临时目录；GIF 在页面保留动画，模型读取首帧。

长期记忆默认关闭。`ChatResident` 只在房间休眠或模型上下文压缩后发出提取触发；`MemoryManager` 保存事实、cache/hot 状态，模型通过 `read_resident_memory` 按需读取。无痕房间不进入记忆。当前 `web` 给两个 Codex Resident 注入同一个 `MemoryManager`，因此不要在文档或代码中假设它们各有独立记忆库。

## 如何写常见改动

1. **新增普通 Resident：** 先看[往返示例](./residents/examples/roundtrip.rs)。在 `residents/` 实现最小 `Resident` 接口，先建立邮箱和可选 Gate，再注册真实 `Arc`；保存回执实例 ID；后台 worker 只处理自己入队的事件；关闭时排空并注销。用 `tests/data_flows.rs` 或相应集成测试验证身份和路由。
2. **新增 LLM 提供者：** 实现 `llm.rs` 的请求/结果协议，声明 `llm` 能力，检查 `origin`，用 `request_id` 对齐异步回复，并自行管理 provider 会话。加入 `chat_rooms.rs` 测试单聊、无 @ 并发、指定 @、后续提及和无痕范围。
3. **改群聊行为：** 从 `ChatResident::send_user_message_with_media`、`run_turn`、`ask` 追踪一次完整轮次。先确认是跨成员并行、同成员串行还是 @ 接力问题，再改调度；不要用前端延迟或 RTDF 全局锁解决。
4. **改模型工具：** 保持模型可见的 schema、Codex 分派、RTDF 工具消息和数据所有者的校验一致。把聊天信息通过 `read_chat_context` 等真实工具返回，不在提示词里伪造工具结果。
5. **改网页：** `web/src/main.rs` 是 HTTP 和运行时组合入口，静态资源由 `include_str!` 编进可执行文件；改 `web/static/*` 后需重建并重启服务，浏览器刷新才会看到新版。页面筛选和显示逻辑在 `app.js`；房间真相仍以 `ChatResident` 为准。
6. **改持久化或无痕：** 同时检查 `chat/storage.rs`、`media.rs`、`memory.rs`、Codex conversation 映射和恢复路径。普通房间需要可恢复；无痕不得写入 Norma 的普通快照或长期记忆。

## 运行与验证

工作区使用 Rust 2024 edition，最低 Rust 1.85。常规检查：

```console
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
node --check web/static/app.js
```

改动只涉及文档时，检查链接、命令、文件路径与当前代码相符，并运行 `git diff --check`。改聊天协议、工具或无痕行为时，以 `residents/tests/chat_rooms.rs` 的跨 Resident 测试为主；改 RDF/RTDF 时以 `tests/data_flows.rs` 为主。`live_codex_can_call_group_member_tool` 需要本机登录的 Codex CLI 和真实模型，默认跳过，不能把它的跳过当成在线调用已验证。

本地网页默认绑定 `0.0.0.0:3000`，当前没有网站用户认证。常用启动配置由 `web/src/main.rs` 读取：`NORMA_WEB_BIND`、`NORMA_CODEX_CWD`、`NORMA_CODEX_BIN`、`NORMA_CODEX_EFFORT`、`NORMA_CODEX_WORKSPACE_WRITE`、`NORMA_DATA_DIR`、`NORMA_THREAD_IDLE_SECS`。默认 Codex 沙箱只读；只有明确设置 `NORMA_CODEX_WORKSPACE_WRITE=1` 才允许工作区写入。
