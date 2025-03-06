# 6.824-golabs-2022

fork from git://g.csail.mit.edu/6.824-golabs-2022

# MIT 6.5840 Lab1: MapReduce

这是 MIT 6.5840 分布式系统课程的第一个实验，实现了一个简化版的 MapReduce 框架。

## 系统架构

MapReduce 系统由以下几个主要组件组成：

1. **Coordinator（协调器）**：负责任务分配和管理
2. **Worker（工作节点）**：执行 Map 和 Reduce 任务
3. **RPC 系统**：用于 Coordinator 和 Worker 之间的通信

整个系统的工作流程如下：

1. Coordinator 初始化，加载输入文件，创建 Map 任务
2. Worker 向 Coordinator 请求任务
3. Coordinator 分配 Map 任务给 Worker
4. Worker 执行 Map 任务，生成中间文件
5. 所有 Map 任务完成后，Coordinator 创建 Reduce 任务
6. Worker 执行 Reduce 任务，生成最终输出文件
7. 所有 Reduce 任务完成后，整个作业结束

## 关键特性

1. **容错机制**：
   - 任务超时检测：如果 Worker 在指定时间内未完成任务，Coordinator 会将任务重新分配给其他 Worker
   - Worker 故障处理：如果 Worker 崩溃，其任务会被重新分配

2. **任务状态管理**：
   - 任务状态包括：空闲（Idle）、进行中（InProgress）、已完成（Completed）
   - 系统阶段包括：Map 阶段、Reduce 阶段、完成阶段

3. **分布式执行**：
   - 多个 Worker 可以并行执行任务
   - 通过 RPC 进行通信

## 代码结构

- `src/mr/coordinator.go`：协调器实现
- `src/mr/worker.go`：工作节点实现
- `src/mr/rpc.go`：RPC 定义和通信相关代码

## 使用方法

### 启动 Coordinator

```bash
go run mrcoordinator.go pg-*.txt
```

这将启动协调器，并将所有匹配 `pg-*.txt` 的文件作为输入。

### 启动 Worker

```bash
go run mrworker.go wc.so
```

这将启动一个工作节点，使用 `wc.so` 中定义的 Map 和 Reduce 函数。你可以启动多个 Worker 来并行处理任务。

## 实现细节

### Coordinator

Coordinator 负责：
- 初始化 Map 和 Reduce 任务
- 分配任务给 Worker
- 监控任务执行状态
- 处理任务超时
- 管理系统阶段转换

### Worker

Worker 负责：
- 向 Coordinator 请求任务
- 执行 Map 或 Reduce 任务
- 生成中间文件或最终输出文件
- 报告任务完成状态

### 任务分配和执行流程

1. Worker 通过 RPC 调用 `AssignTask` 向 Coordinator 请求任务
2. Coordinator 根据当前阶段分配 Map 或 Reduce 任务
3. Worker 执行任务，生成输出文件
4. Worker 通过 RPC 调用 `UpdateTask` 报告任务完成
5. Coordinator 更新任务状态，必要时转换系统阶段

## 测试

可以使用以下命令运行测试：

```bash
cd src/main
go test -run TestBasic
```

这将运行基本功能测试，验证系统是否正确实现了 MapReduce 功能。

## 扩展功能

1. **负载均衡**：可以实现更智能的任务分配策略，考虑 Worker 的处理能力
2. **容错增强**：增加更多的容错机制，如检查点和恢复
3. **性能优化**：优化中间数据的处理和传输

## 参考资料

- [MapReduce: Simplified Data Processing on Large Clusters](https://research.google/pubs/pub62/) - Google 的原始 MapReduce 论文
- [MIT 6.5840 课程网站](https://pdos.csail.mit.edu/6.824/)
