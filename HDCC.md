# HDCC 代码导读（从 0 到 1，面向新手）

> 目标读者：
> - 不熟悉本项目代码
> - 对数据库和并发控制了解有限
> - 希望按“可以上手调试/改代码”的方式理解 HDCC

---

## 1. 项目在做什么（先建立全局心智模型）

HDCC 是一个分布式 OLTP 测试系统中的**混合并发控制**实现，核心思想是：

- 同一套系统里，事务可以按策略走**确定性路径（Calvin）**或**非确定性路径（Silo/OCC 类）**。
- 系统支持多个 workload（YCSB / TPCC / PPS / DA）。
- 通过线程、消息队列、网络通信把一个事务在多节点上推进到提交/回滚。

README 的一句话已经概括：HDCC 混合使用 Calvin 和 Silo。并说明它基于 Deneva。  
（建议第一次阅读先只看 README 与运行方式）

---

## 2. 目录级别快速导航（按职责分组）

- `system/`：系统主干（启动、线程、事务管理、消息队列、调度等）
- `client/`：客户端主程序与查询生成队列
- `benchmarks/`：具体 workload 的 query/txn 逻辑（YCSB/TPCC/PPS/DA）
- `storage/`：表、行、索引
- `transport/`：网络通信层
- `statistics/`：统计指标
- `concurrency_control/`：各种 CC 算法的行级/事务级实现

> 学习建议：先读 `system + client + benchmarks` 主线，再回头钻 `storage + concurrency_control` 细节。

---

## 3. 启动流程（服务端）

服务端入口：`system/main.cpp`。

高层步骤：

1. **解析参数**：`parser(argc, argv)`
2. **初始化全局模块**：stats / global manager / transport / simulation
3. **按 WORKLOAD 构造 workload** 并 `m_wl->init()`（YCSB/TPCC/PPS/DA）
4. **初始化执行基础设施**：work queue、abort queue、message queue、txn/row/access/query/msg 各类 pool、txn_table
5. **按算法初始化控制组件**：  
   - CALVIN/HDCC/SNAPPER：`seq_man.init()`  
   - HDCC：`cc_selector.init()`  
   - 其他算法（如 MAAT/SSI/WSI/WOOKONG）初始化各自 manager/table
6. **创建并启动线程**：worker / input / output / abort / log / sequencer / lock / conflict 等（取决于 `CC_ALG`）

这是整个 server 生命周期最关键的一条链。

阅读这个流程时建议把它理解成三层：

- **启动层**：参数解析 + 基础模块 init（决定本轮实验的运行时配置）；
- **运行层**：队列/pool/table（决定事务在系统里如何被承接与复用对象）；
- **策略层**：sequencer / cc_selector / 各 CC manager（决定事务实际执行路径与冲突处理方式）。

---

## 4. 启动流程（客户端）

客户端入口：`client/client_main.cpp`。

高层步骤：

1. **参数解析与节点角色确认**：`parser(argc, argv)` 后校验 `g_node_id >= g_node_cnt`（客户端节点编号与服务端节点区间分离）
2. **初始化基础模块**：stats / transport / workload / simulation / client_man
3. **初始化通信与对象容器**：`work_queue`、`msg_pool`、`msg_queue`
4. **初始化 `client_query_queue`**：生成（或装载）事务模板，作为后续持续发送的输入源
5. **创建线程并进入运行**：  
   - `ClientThread`：主发压线程，循环取 query 并发 `CL_QRY`  
   - `InputThread`：接收服务端返回消息  
   - `OutputThread`：网络发送/出队  
   - （可选）`DynamicThread`：动态负载相关逻辑

客户端核心职责是：

- 获取下一个 query（事务模板）
- 封装为消息 `CL_QRY`
- 发给目标 server 节点

进一步看，客户端其实在做三件彼此解耦的事：

- **流量塑形**：通过发送线程节奏、inflight 上限、workload 参数把“理论负载”变成“可控请求流”；
- **协议适配**：把本地 query 统一包装为消息类型（而不是直接函数调用），为跨节点通信与统计留接口；
- **观测闭环**：接收响应并更新统计，支持吞吐/时延分析与实验复现。

为什么要把这部分先读透：  

后续你看到的 CC 分流、远程子事务、2PC 等复杂逻辑，入口几乎都从客户端发出的 `CL_QRY` 开始；客户端是否稳定、是否按预期发压，直接决定了你在服务端观察到的行为是否可信。

---

## 5. 全局配置体系（宏 + 运行时覆盖）

### 5.1 编译期默认值

大量参数在 `system/global.cpp` 用 `config.h` 宏初始化，例如：

- 节点数、线程数、inflight 上限
- workload 参数（YCSB/TPCC）
- 各类开关（日志、网络等）

### 5.2 运行时命令行覆盖

`system/parser.cpp` 提供命令行参数覆盖（例如 `-nid` / `-t` / `-tif` 等）。

本次你关注的负载日志相关参数也在这里：

- `-lsleINT`：是否开启日志
- `-lslf STRING`：日志文件路径
- `-lslsINT`：采样率

### 5.3 实验时建议

先固定编译期宏（稳定基线），再通过运行参数做实验矩阵扫描。

### 5.4 只修改 `config.h` 是否需要重新编译？

结论：**通常需要重新编译**。  
原因是大量配置宏会参与编译期分支或静态初始化，例如：

- `#if CC_ALG == ...` / `#if WORKLOAD == ...` 这种条件编译；
- 线程数、功能开关等会影响编译产物路径；
- `global.cpp` 中的 `g_*` 默认值由宏初始化。

因此只改 `config.h`，一般不是“重启就生效”，而是“重编译后生效”。

### 5.5 `config.h`、`global.cpp`、`parser.cpp` 的关系（建议牢记）

配置链路可以理解为：

```text
config.h(宏默认值)
    ↓
global.cpp 初始化 g_* 运行时变量
    ↓
parser.cpp 解析命令行覆盖 g_*
    ↓
main/client_main 初始化模块并消费 g_*
```

也就是说：

- `config.h`：给默认值、决定部分编译期行为；
- `global.cpp/.h`：定义并导出运行时变量；
- `parser.cpp`：提供运行时覆盖入口。

### 5.6 如果要新增一个全局变量或可配置参数，该怎么改？

#### 场景 A：新增全局变量（仅内部默认使用）

1. `system/global.h` 增加 `extern` 声明；  
2. `system/global.cpp` 增加定义和默认值（通常来自宏）；  
3. `config.h` 增加默认宏；  
4. 在业务代码中读取该 `g_*` 变量。

#### 场景 B：新增“可命令行覆盖”的配置项

在场景 A 基础上，再做：

5. `system/parser.cpp::print_usage()` 增加参数帮助；  
6. `system/parser.cpp::parser()` 增加解析逻辑并覆盖 `g_*`；  
7. 启动打印中输出最终值（便于实验复现与排错）。

### 5.7 目前最常修改的配置项（按类别）

#### 集群规模 / 线程
- `NODE_CNT`, `PART_CNT`
- `THREAD_CNT`, `REM_THREAD_CNT`, `SEND_THREAD_CNT`
- `CLIENT_NODE_CNT`, `CLIENT_THREAD_CNT`

#### 压力与时长
- `MAX_TXN_IN_FLIGHT`
- `MAX_TXN_PER_PART`
- `DONE_TIMER`, `WARMUP_TIMER`

#### Workload 参数
- YCSB：`SYNTH_TABLE_SIZE`, `REQ_PER_QUERY`, `ZIPF_THETA`, `PERC_MULTI_PART`
- TPCC：`NUM_WH`, `PERC_PAYMENT` 及相关规模参数

#### 并发控制 / 混合策略
- `CC_ALG`
- HDCC/SNAPPER 的分流相关阈值和比例参数

#### 本次新增日志相关
- `LOAD_STMT_LOG_ENABLE`
- `LOAD_STMT_LOG_FILE`
- `LOAD_STMT_LOG_SAMPLE`

### 5.8 配置修改的推荐实践

1. 先在 `config.h` 固定基线（算法、workload 类型、线程上限）；  
2. 重编译得到基线二进制；  
3. 用 CLI 参数做小范围扫描（如 inflight、zipf、mpr、日志采样）；  
4. 保留完整启动命令和启动打印，确保实验可复现。  

---

## 6. 一条事务请求如何流动（端到端）

下面这条主线最重要：

### Step A：生成 query（事务模板）

`client_query_queue` 在 init 阶段预生成（或 DA 用队列动态生成）事务模板。

补充：这里的“输入”不是 SQL 文本，而是 `BaseQuery` 的具体子类对象（如 `YCSBQuery/TPCCQuery`）。这能减少运行期解析开销，并让 query 直接携带访问参数，便于参与者计算与并发控制分流。对 DA workload，生成也不是固定数组，而是动作序列动态入队，因此 `get_next_query()` 在 DA 分支会从队列 `pop_data()`。

### Step B：客户端发送

`ClientThread::run()` 循环：

- 选择目标 server
- `get_next_query()` 拿 query
- `Message::create_message(..., CL_QRY)`
- `msg_queue.enqueue(..., dest_node)`

补充：客户端会按负载模型控制发送节奏（`LOAD_MAX` / `LOAD_RATE`）并维护 inflight 约束，避免无上限施压导致系统失真。使用 `CL_QRY` 消息（而非直接 RPC）也让客户端与服务端异步解耦，便于统计网络/排队延迟，并为后续重试、复制、限流保留扩展点。

### Step C：服务端接收并入工作队列

input/send 线程把消息推给 worker 的队列。

补充：把“接收”和“执行”拆成 input/send 与 worker 两类线程，可以减少网络线程阻塞、降低执行线程上下文切换；同时队列层可记录排队时间，便于把总时延拆成网络、排队、执行三个阶段。

### Step D：worker 分派执行路径

`WorkerThread::process(...)` 对 `CL_QRY` 进行分发：

- Calvin（或 HDCC/SNAPPER 中被标记为 CALVIN）→ `process_calvin_rtxn`
- 其他路径 → `process_rtxn`

补充：这一步不是普通函数分发，而是并发控制协议入口。HDCC/SNAPPER 下由 `msg->algo` 决定走哪条路径，而 `msg->algo` 来自 sequencer 的分流结果。

### Step E：TxnManager 执行状态机

`process_rtxn` 会把消息拷贝到 `txn_man`，进入具体 workload 的 `run_txn()` / `run_txn_state()`。

补充：`run_txn_state()` 的核心是把事务拆成“可暂停、可远程、可恢复”的微步骤。分布式事务中步骤常依赖远端返回，不可能总是一次性跑完；不同 CC 算法也需要在特定阶段插入验证/锁处理。TPCC 里常见的 `PAYMENT0..5`、`NEWORDER0..` 本质上就是这种阶段化状态机。

### Step F：提交/回滚 + 2PC/远程协作

最终调用 `TxnManager::start_commit()/commit()/abort()`，涉及 prepare/finish 消息、锁释放、统计等。

补充：`start_commit()` 只是进入提交协议，不等于立刻提交。典型流程是 prepare → 汇总 ACK → commit/abort → 释放锁/更新统计（必要时写日志），所以事务命运由协议收敛结果决定。这里还要分清两个标识：`txn_id` 负责事务实例关联（txn_table、消息、统计归并），`batch_id` 负责 Calvin/HDCC 路径的批次协调与 ACK 收敛；同时 `IS_LOCAL(txn_id)` 用于区分本地主事务与远程参与者（本地驱动主流程并回客户端，远程执行子步骤并回传 `RQRY_RSP/RACK_*`）。

> 阅读指引：看不懂分流回 Step D；看不懂 TPCC 状态机回 Step E；看不懂 commit/abort 回 Step F。把这 6 个 step 连起来，后续读任何算法实现会顺很多。

---

## 7. HDCC 的“Calvin / Silo 分流”在哪里

关键文件：

1. `system/cc_selector.cpp`：策略判断（get_best_cc）
2. `system/sequencer.cpp`：实际路由与 batch 处理

在 HDCC 中：

- `get_best_cc(msg)` 返回 SILO -> 直接入普通 work queue（非确定性执行）
- 返回 CALVIN -> 进入 sequencer 的 participants + batch 分发流程

因此，**cc_selector 是“决策层”**，**sequencer 是“执行编排层”**。

---

## 8. workload 层如何阅读

### 8.1 YCSB

- `ycsb_wl.cpp`：建表/初始化数据
- `ycsb_query.cpp`：生成访问请求（读写 key 列表）
- `ycsb_txn.cpp`：状态机执行 + 远程请求发送

YCSB 相对简单，非常适合先读它理解主框架。

### 8.2 TPCC

- `tpcc_query.cpp`：按比例生成 PAYMENT / NEW_ORDER / ... 事务模板
- `tpcc_txn.cpp`：复杂状态机推进（`run_txn_state`, `next_tpcc_state`）

TPCC 的代码体现了“存储过程式事务程序”的思想：

- 客户端不是逐条 SQL 交互
- 服务端按预定义阶段推进事务

### 8.3 PPS / DA

- PPS：业务逻辑更偏图式关联查询
- DA：动作序列（action sequence）型 workload

---

## 9. 存储层（你需要知道到什么程度）

### 9.1 表与行

- `table_t::get_new_row()`：分配 row
- `row_t::init_manager()`：按 `CC_ALG` 安装行级并发控制器（Row_silo/Row_hdcc/Row_occ/...）

这意味着同一个上层事务代码，可以在不同 CC 算法下复用行访问 API。

### 9.2 索引

`IndexHash` 提供 `index_insert/index_read` 等接口，内部 bucket+latch 管理。

---

## 10. 消息与网络

### 10.1 本地消息队列

`system/msg_queue.cpp`：发送线程队列，维护消息排队统计。

### 10.2 节点间通信

`transport/transport.cpp`：

- 读取 `ifconfig.txt`
- 构造端口与 socket 名称
- 建立 bind/connect

实际传输通过 nanomsg 封装。

---

## 11. 通用事务骨架（最值得重点读）

`system/txn.cpp` 负责：

- `commit()/abort()` 的通用收尾
- `start_commit()` 的 prepare/finish 协议
- 锁/版本/日志与统计的收敛

你可把 workload 的 `*_txn.cpp` 看成“业务步骤”，把 `txn.cpp` 看成“事务协议骨架”。

---

## 12. 你现在最推荐的阅读顺序（一步一步）

### 第 1 轮：只看主线，不抠细节

1. `README.md`
2. `system/main.cpp` / `client/client_main.cpp`
3. `system/parser.cpp` / `system/global.*`
4. `system/client_thread.cpp`
5. `system/worker_thread.cpp`（先只看 CL_QRY 分支）

### 第 2 轮：看一个 workload 跑通

6. `benchmarks/ycsb_query.cpp`
7. `benchmarks/ycsb_txn.cpp`
8. `system/txn.cpp`

### 第 3 轮：理解 HDCC 差异化价值

9. `system/cc_selector.cpp`
10. `system/sequencer.cpp`
11. `benchmarks/tpcc_query.cpp` + `benchmarks/tpcc_txn.cpp`

### 第 4 轮：深入存储/并发控制细节

12. `storage/row.cpp` / `storage/index_hash.cpp`
13. `concurrency_control/row_*.cpp`

---

## 13. 关于“负载日志功能”在主线中的位置

你新增的日志逻辑位于 query 生成阶段（`client_query.cpp` / `da_query.cpp`），
不是事务执行阶段。因此它主要用于“输入工作负载可观测性”，不是执行器内部每一步的审计日志。

如果未来要扩展为“执行级日志”，建议挂载点放在：

- `run_txn_state()` 每状态推进时
- `get_row()/index_read()` 访问时
- `commit/abort` 关键路径

---

## 14. 当前文档后续可继续扩展的方向

后面你让我继续补充时，可以从下面挑一个方向我展开：

1. **TPCC NEW_ORDER 端到端逐行追踪**（函数级调用图）
2. **HDCC 下 CALVIN/SILO 分流真实案例**（给具体输入，解释为何分流）
3. **消息类型全字典**（CL_QRY/RQRY/RACK_PREP/... 一览）
4. **并发控制器 row manager 对照表**（每种算法访问 row 的差异）
5. **统计指标含义大全**（stats 字段对应的阶段意义）

---

## 15. 一句话总结

如果把 HDCC 看成“工厂流水线”——

- `client_query` 是“订单生成”
- `client_thread + msg_queue + transport` 是“物流系统”
- `worker_thread + *_txn` 是“加工车间”
- `sequencer + cc_selector` 是“调度中枢（决定走快线还是稳线）”
- `txn.cpp` 是“质检+出厂流程（提交/回滚协议）”
- `storage/*` 是“仓库与工装”

掌握这张图后，再看任何单点代码都不会迷路。

---