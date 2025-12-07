# Permit 插件重新执行实现总结

## 实现概述

在 Reserve、PreBind、Bind 阶段失败并尝试候选节点时，已实现 Permit 插件的重新执行机制。

## 实现的修改

### 1. 新增辅助函数

#### `cancelPermitWait`
- **位置**：`pkg/scheduler/schedule_one.go`
- **功能**：取消 Pod 的 Permit Wait 状态
- **用途**：在切换到候选节点前，取消之前节点的 Permit Wait

#### `reexecutePermitForCandidate`
- **位置**：`pkg/scheduler/schedule_one.go`
- **功能**：为候选节点重新执行 Permit 插件
- **返回值**：Permit 状态和是否需要等待
- **处理**：
  - 如果 Permit 返回 `Wait`，标记需要等待
  - 如果 Permit 失败或拒绝，返回错误状态
  - 如果 Permit 成功，返回 nil

### 2. Reserve 阶段失败处理

**位置**：`pkg/scheduler/schedule_one.go:215-280`

**修改内容**：
1. 在尝试候选节点前，调用 `cancelPermitWait` 取消 Permit Wait
2. 修改 `tryCandidateNodesForReserve` 函数：
   - 在 Reserve 成功后，重新执行 Permit 插件
   - 返回 Permit 状态（如果返回 Wait）
3. 在调用 `tryCandidateNodesForReserve` 后，检查 Permit 状态：
   - 如果 Permit 失败，清理状态并处理失败
   - 如果 Permit 返回 Wait，状态会在绑定阶段处理

### 3. PreBind 阶段失败处理

**位置**：`pkg/scheduler/schedule_one.go:380-406`

**修改内容**：
1. 在尝试候选节点前，调用 `cancelPermitWait` 取消 Permit Wait
2. 修改 `tryCandidateNodesForPreBind` 函数：
   - 在 Reserve 成功后，重新执行 Permit 插件
   - 如果 Permit 返回 Wait，调用 `WaitOnPermit` 等待
   - 如果 Permit 失败，清理状态并继续下一个候选节点

### 4. Bind 阶段失败处理

**位置**：`pkg/scheduler/schedule_one.go:429-540`

**修改内容**：
1. 在尝试候选节点前，调用 `cancelPermitWait` 取消 Permit Wait
2. 在循环中，为每个候选节点：
   - 重新执行 Assume
   - 重新执行 Reserve
   - **重新执行 Permit**（新增）
   - 如果 Permit 返回 Wait，调用 `WaitOnPermit` 等待
   - 重新执行 PreBind
   - 重新执行 Bind

## 执行流程

### Reserve 阶段失败时的流程

```
Reserve 失败
  ↓
取消 Permit Wait（如果存在）
  ↓
清理首选节点状态（Unreserve + ForgetPod）
  ↓
尝试候选节点：
  - Assume
  - Reserve
  - **重新执行 Permit** ← 新增
  - 如果 Permit 返回 Wait，状态会在绑定阶段处理
  ↓
继续后续流程（Permit、绑定等）
```

### PreBind 阶段失败时的流程

```
PreBind 失败
  ↓
取消 Permit Wait（如果存在）
  ↓
清理首选节点状态（Unreserve + ForgetPod）
  ↓
尝试候选节点：
  - Assume
  - Reserve
  - **重新执行 Permit** ← 新增
  - **如果 Permit 返回 Wait，调用 WaitOnPermit** ← 新增
  - PreBind
  - Bind
  - PostBind
  ↓
成功绑定
```

### Bind 阶段失败时的流程

```
Bind 失败
  ↓
取消 Permit Wait（如果存在）
  ↓
遍历候选节点：
  - Assume
  - Reserve
  - **重新执行 Permit** ← 新增
  - **如果 Permit 返回 Wait，调用 WaitOnPermit** ← 新增
  - PreBind
  - Bind
  - PostBind
  ↓
成功绑定
```

## 关键点

### 1. Permit Wait 的处理

- **取消等待**：在切换到候选节点前，必须取消之前节点的 Permit Wait
- **重新等待**：如果新的 Permit 返回 Wait，需要调用 `WaitOnPermit` 等待

### 2. 状态清理

- 切换节点前，必须清理之前节点的状态：
  - Unreserve
  - ForgetPod
  - 取消 Permit Wait

### 3. 错误处理

- 如果 Permit 失败或拒绝，清理状态并继续下一个候选节点
- 如果 `WaitOnPermit` 失败，清理状态并继续下一个候选节点

## 测试建议

1. **测试 Permit 插件依赖节点的情况**：
   - 创建 Permit 插件，基于节点标签做决策
   - 验证切换到候选节点时，Permit 会重新执行

2. **测试 Permit Wait 的情况**：
   - 创建 Permit 插件，返回 Wait
   - 验证切换到候选节点时，会取消之前的 Wait 并重新等待

3. **测试 Reserve/PreBind/Bind 阶段失败的情况**：
   - 模拟各阶段失败
   - 验证 Permit 会重新执行

## 注意事项

1. **性能影响**：重新执行 Permit 会增加调度延迟，但这是必要的，因为 Permit 可能依赖节点特定信息

2. **状态一致性**：确保在切换节点时，所有状态都被正确清理和重新初始化

3. **Permit Wait 超时**：如果 Permit 返回 Wait，需要确保有超时机制，避免无限等待

## 总结

已完整实现 Permit 插件在 Reserve、PreBind、Bind 阶段失败并尝试候选节点时的重新执行机制。这确保了：

1. ✅ Permit 插件会为每个候选节点重新执行
2. ✅ Permit Wait 状态会被正确处理（取消和重新等待）
3. ✅ 状态清理和错误处理都已实现
4. ✅ 与现有的候选节点机制完全集成

