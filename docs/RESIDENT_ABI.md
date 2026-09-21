# Resident WASM ABI v1

本文只定义 WASM Resident 的隔离 ABI，不适用于实现 Rust `Resident` trait 的受信任 Native Resident。WASM Resident 是零 import 的 WebAssembly 模块；RDF 在接受注册时编译模块，并在发现任何 import（包括 WASI）时拒绝整个提交。

## 必需导出

| 导出 | 签名 | 含义 |
| --- | --- | --- |
| `memory` | WebAssembly linear memory | Resident 私有内存 |
| `village_abi_version` | `() -> i32` | 必须返回 `1` |
| `village_alloc` | `(i32 length) -> i32 pointer` | 为宿主输入分配连续内存 |
| `village_receive` | `(i32 pointer, i32 length) -> i64` | 处理 JSON 调用并返回响应位置 |
| `village_dealloc` | `(i32 pointer, i32 length) -> ()` | 释放输入或输出缓冲区 |

可选导出 `village_shutdown: () -> ()`。RTDF 在热替换或硬休眠关闭信箱时调用它；它不能阻止 Store 随后被丢弃。

## 数据格式

宿主把 UTF-8 JSON 编码的 `ResidentInvocation` 写入 `village_alloc` 返回的区域，再调用 `village_receive`。调用中的 `ResidentContextSnapshot.directory` 是当前 RDF 快照的安全数据投影；它不包含任何宿主对象。调用结束后宿主调用 `village_dealloc` 释放输入。

`village_receive` 返回的 `i64` 使用以下布局：

```text
bits 63..32 = output pointer (u32)
bits 31..0  = output length  (u32)
```

输出区域必须包含 UTF-8 JSON 编码的 `ResidentResponse`。宿主复制并解码后调用 `village_dealloc` 释放输出。成功响应只能包含 `ResidentEffect`；correlation ID、message ID 和 hop 等传输字段由 RTDF 维护。

## Capability 规则

- 模块必须没有 import；当前 ABI 没有 WASI，也没有 host function。
- 每个 AgentThread / Resident exposure 使用独立 Store 和 linear memory。
- `ResidentContextSnapshot` 是本次投递的数据副本。保存它不会得到 Layout 的后续访问权。
- `ResidentDirectorySnapshot` 只公开门牌、接受的消息类型和能力标签。它不是 RDF、注册表句柄或实际路由表。
- Resident 可在自身 linear memory 中保存私有状态，直到热替换或 AgentThread 硬休眠。
- Resident 只通过返回 `Emission` 寻址另一个 Resident；实际数据包由 RTDF 生成并投递。
- `ResidentLimits` 限制每个实例的内存、单次输入/输出、单次执行 fuel 与 mailbox 容量。
