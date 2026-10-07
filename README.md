# robot-topic-delivery-java

进程内机器人主题通信与可靠投递组件（Spring Boot 3 / Java 21+）。不依赖任何外部消息代理或外部服务，
提供层级主题发布订阅、可靠投递状态机、有界队列背压、连接中断与恢复补投、**重试退避**、
**失败消息留存（死信）与重投**，以及可核验的统计诊断。

## 主题与匹配规则

- 主题名以 `/` 分层，如 `robot/arm/position`。每层非空、不含空白字符、不含通配符；非法主题在发布时被拒绝（`INVALID_TOPIC`）。
- 订阅过滤器支持三种形式：
  - 精确匹配：`robot/arm/position` 只匹配同名主题；
  - 单层通配 `+`：`robot/+/position` 匹配 `robot/arm/position`，不匹配 `robot/arm/joint/position`；
  - 多层通配 `#`（必须在最后一层）：`robot/#` 匹配 `robot` 及其全部子层级。
- 负载经 JSON 编码（Jackson）。`null` 负载被拒绝（`INVALID_PAYLOAD`），无法序列化的负载被拒绝（`ENCODE_FAILURE`）。
- 拒绝原因彼此可区分（见 `RejectReason`），且单条消息的拒绝不影响同批其他消息或其他订阅者。

## 投递语义与状态流转

每条消息对每个匹配的订阅产生一条独立投递，状态机如下：

```
PENDING --(派发)--> IN_FLIGHT --(确认)--> ACKED           [终态]
                       |  ^
                       |  | ack 超时且重试未耗尽
                       |  +--> BACKOFF --(退避结束)--> PENDING（再派发）
                       |
                       +--(重试耗尽)--> DEAD              [终态，留存死信]
```

- 每个订阅拥有独立的有界队列与单派发线程：同一订阅内严格按序投递，重试（含退避等待）会阻塞后续消息（队头阻塞语义）。
- 确认通过 `Delivery.ack()` 完成，**幂等**：
  - 重复确认：计入 `duplicateAcks`，不重复投递、不改变状态；
  - 旧 attempt 的乱序/迟到确认、退避窗口（`BACKOFF`）中到达的确认：计入 `lateAcks`，不回退当前状态；
  - 已终止（ACKED/DEAD）或订阅已移除后的确认：计入 `lateAcks`，终态不复活。
- 监听器抛异常视为未确认，走超时→退避→重试路径。
- 总投递次数 = 1 + `maxRetries`，之后进入 `DEAD` 并完整留存（见下）。

## 重试退避（重试之间拉开间隔）

确认超时后不再立即重打，而是先进入 `BACKOFF` 退避等待，逐次变长：

```
第 n 次重试前等待 = min(retry-backoff-max-millis,
                        retry-backoff-initial-millis * retry-backoff-multiplier^(n-1))
```

- `retry-backoff-initial-millis=0` 时退避时长恒为 0，**等价于历史的立即重试行为**（老的 4 参数 `BrokerConfig`
  构造即采用该设置，既有程序与用例零改动）。
- 等待基于 `Object.wait` 的定时等待（非自旋），**不空转占满 CPU**；退避中线程状态为 `TIMED_WAITING`。
- 每个订阅独立派发线程：一个订阅的退避等待**不会连累其他订阅**。
- **退避期间断连**：退避按墙上时钟照常走完，但不会投递；退避结束后若仍断连则等待恢复，
  `reconnect` 后只补投一次——既不丢消息也不重复投。断连不改变退避结果。
- **退避期间移除订阅（`unsubscribe`）**：该消息与队列中的消息一起被确定性地计为 `dropped`，
  不再重投、也不进入死信留存（留存只针对“重试耗尽”）。
- 退避记账：每进入一次退避计 `backoffWaits`，计划等待毫秒累计入 `backoffMillis`；诊断中可看到 `stateBackoff`。

## 失败留存（死信）与重投

重试耗尽进入 `DEAD` 的消息会被**完整留存**到一个有界的进程内死信存储，记录包含：
原始主题、原始负载（JSON 字节）、所属订阅、实际投递次数（1+重试次数）、终结时间、记录 id、
以及重投链路信息（`originMessageId`、状态、重投后的新消息 id）。

```java
broker.listDeadMessages();                       // 全部留存（按终结时间从旧到新）
broker.listDeadMessages(subscriptionId);         // 某个订阅的留存
broker.deadMessage(recordId);                    // 按记录 id 查看单条
RedeliveryResult r = broker.redeliverDeadMessage(subscriptionId, originalMessageId);
```

### 重投是一次“全新投递”

- 重投生成**全新的消息 id**，重新入队并完整走一遍 确认 → 超时 → 退避 → 重试 → 终止 流程；
  新消息携带原始链路 `originId` 便于追溯。
- 原留存记录被原子标记为 `REDRIVEN` 并继续留存：旧记录**不复活**，同一原始消息**不会重复生效**。
- 重投入队同样计入 `enqueued`（并单列 `reEnqueued`），因此逐批守恒关系不变（见下）。
- 重投后若再次耗尽重试，会作为一条全新终止消息再留存一条（`originId` 相同，链路可追溯）。

### 重投的边界（挡下原因彼此可区分，见 `RedeliveryRejectReason`）

| 原因 | 触发情形 |
| --- | --- |
| `SUBSCRIPTION_GONE` | 目标订阅已不存在（从未注册或已被移除） |
| `RECORD_NOT_FOUND` | 该订阅下没有由该原始消息终结而来的留存记录（已正常 ACKED、从未终止、已被容量淘汰） |
| `STILL_IN_FLIGHT` | 原始消息仍在投递路上（队列 / 在途 / 退避中），尚不可重投 |
| `ALREADY_REDELIVERED` | 该留存记录已经重投过，不能再次重投 |
| `QUEUE_FULL` | 重投那一刻目标订阅队列正好满，沿用既有**确定性背压拒绝**，不被悄悄吞掉；记录保持 `RETAINED`，稍后可再试 |
| `BROKER_CLOSED` | broker 已关闭 |

并发对同一条留存记录发起重投时，只有一次会被接受，其余全部得到 `ALREADY_REDELIVERED`，
绝不会重复入队/重复生效。

### 留存容量上限与溢出取舍

容量由 `broker.dead-letter-capacity` 限制（全 broker），溢出策略由
`broker.dead-letter-overflow-policy` 决定：

- `EVICT_OLDEST`（默认）：新终止消息照常留存，**淘汰最旧的一条**记录，淘汰数计 `deadEvicted`；
- `REJECT_NEW`：保留既有记录，新终止消息**不留存**，计 `deadArchiveRejected`。

记账关系（任意时刻可核对）：

```
dead == deadArchived + deadArchiveRejected
deadArchived == 当前留存数(RETAINED + REDRIVEN) + deadEvicted
```

被淘汰的记录不再可列出/重投（再请求按 `RECORD_NOT_FOUND` 挡下）。

## 背压与拒绝策略

- 每个订阅的队列容量为 `broker.queue-capacity-per-subscription`。队列满时，发布对该订阅返回
  `EnqueueResult(enqueued=false)` 并计入 `enqueueRejected`，**不影响其他订阅**——慢订阅者不会拖垮快订阅者。
  **死信重投撞上队列满时走完全相同的确定性拒绝路径**（原因 `QUEUE_FULL`）。
- 发布本身是同步、原子的：主题/负载/编码校验失败则整体拒绝（`PublishReceipt.accepted=false`，
  原因见 `rejectReason`），校验通过则一定接受（`accepted=true`），各订阅的入队结果逐项可查。
- 订阅被移除（`unsubscribe`）时，其在途、退避中与在队消息被确定性地计为 `dropped`，不会半途中断产生歧义。

## 连接中断与恢复

- `disconnect(subscriptionId)`：暂停该订阅的派发（退避中的消息也不会在恢复前投递）；已入队与在途未确认消息全部保留，不投递、不丢失。
- `reconnect(subscriptionId)`：恢复派发并补投全部未确认消息（在途消息以递增的 attempt 重投），
  补投数量计入 `redeliveredOnReconnect`。重复投递只会以“同一消息的新一轮 attempt”形式出现，
  确认仍然幂等，不会产生重复生效。

## 统计与诊断

`broker.stats().snapshot()` 提供单调递增计数：

| 计数器 | 含义 |
| --- | --- |
| `published` / `publishRejected` | 发布被接受 / 被拒绝 |
| `enqueued` / `enqueueRejected` | 入队成功（含重投 `reEnqueued`）/ 背压拒绝（含重投撞满） |
| `delivered` | 实际投递次数（含重试/重投） |
| `acked` / `duplicateAcks` / `lateAcks` | 有效确认 / 重复确认 / 迟到确认（含退避窗口中的确认） |
| `timeouts` / `retries` | 确认超时 / 重试 |
| `backoffWaits` / `backoffMillis` | 进入退避等待的次数 / 累计计划退避毫秒 |
| `dead` | 重试耗尽终止的消息数 |
| `dropped` | 订阅移除时丢弃的在途/退避/在队消息 |
| `redeliveredOnReconnect` | 恢复时承诺补投的在途消息数 |
| `deadArchived` / `deadArchiveRejected` / `deadEvicted` | 成功留存 / 容量满（REJECT_NEW）未留存 / 被淘汰 |
| `redriveRequested` / `redriveAccepted` / `redriveRejected` | 重投请求数 / 接受数 / 被挡下数 |
| `reEnqueued` | 重投产生的全新投递入队数（已包含在 `enqueued` 中） |

**守恒不变量**（静默后可验证）：

```
enqueued == acked + dead + dropped                    （无在途/退避消息时）
dead == deadArchived + deadArchiveRejected
redriveRequested == redriveAccepted + redriveRejected
```

`broker.diagnostics()` 提供每个订阅的队列深度、在途数、各状态计数
（新增 `stateBackoff`）、死信留存/已重投数（`deadRetained` / `deadRedriven`）与连接状态。

## 配置项（application.properties）

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `broker.queue-capacity-per-subscription` | 64 | 每订阅队列容量，满时背压拒绝 |
| `broker.ack-timeout-millis` | 500 | 确认超时，超时触发重试 |
| `broker.max-retries` | 3 | 最大重试次数，耗尽进入 DEAD 并留存 |
| `broker.dispatch-interval-millis` | 5 | 派发循环空转间隔 |
| `broker.retry-backoff-initial-millis` | 100 | 首次重试前退避；**0 = 立即重试（旧行为）** |
| `broker.retry-backoff-multiplier` | 2.0 | 退避倍数，逐次变长；1.0 = 固定间隔 |
| `broker.retry-backoff-max-millis` | 5000 | 单次退避等待上限 |
| `broker.dead-letter-capacity` | 256 | 死信留存条数上限（全 broker） |
| `broker.dead-letter-overflow-policy` | EVICT_OLDEST | 溢出策略：`EVICT_OLDEST` 或 `REJECT_NEW` |

### 与既有语义的兼容范围

**保持不变**：主题/过滤规则、发布校验与可区分拒绝、逐订阅有界队列与背压语义、
确认幂等（重复/乱序/迟到）、断连暂停与恢复补投、`unsubscribe` 的确定性 `dropped`、
逐批守恒关系。旧的 4 参数 `BrokerConfig(queue, ackTimeout, maxRetries, dispatchInterval)`
构造仍保留，退避自动取 0（立即重试）、死信容量 256 且淘汰最旧。

**会受影响的默认行为**：经 Spring `application.properties` 装配的默认实例，重试不再“立刻重打”，
而是按 100ms 起步、2 倍递增退避（上限 5s）——这是本次有意引入的改变；若需旧节奏，把
`broker.retry-backoff-initial-millis` 设为 `0`。另外，重试耗尽终止的消息现在会额外留存一份死信记录
（此前仅停留在 DEAD 终态计数），可通过容量与溢出策略控制其内存占用。

## 本地验证

```bash
mvn test
```

测试输出按业务打印关键输入、状态迁移、计数快照与判定依据，并给出每个测试类与整体的通过情况。覆盖：

- `TopicMatchingTest`：精确/单层/多层通配匹配，非法主题与非法过滤器拒绝；
- `BrokerReliabilityTest`：发布校验的可区分拒绝、确认流转与计数守恒、重复/乱序/迟到确认、
  重试耗尽进入 DEAD；
- `BrokerConcurrencyTest`：多线程并发发布至多订阅者、慢订阅者队列饱和背压（且不影响快订阅者）、
  断连期间不投递不丢失、恢复补投、在途未确认消息的恢复重投、取消订阅的确定性丢弃、
  发布过程中动态增删订阅；
- `RetryBackoffTest`：退避时长逐次变长的严格下界断言、退避等待不忙等自旋（线程状态采样）、
  一个订阅退避不连累其他订阅、退避中断连恢复后只补投一次、退避中移除订阅的确定性丢弃；
- `DeadLetterRedeliveryTest`：死信字段完整留存与防御性拷贝、留存→重投全新投递且不复活不重复、
  四类挡下原因可区分、32 线程并发重投仅一次生效、重投撞队列满被 `QUEUE_FULL` 挡下且稍后可成功、
  两种溢出策略的取舍与记账、重投再终结的链路追溯与守恒。

也可单独运行某一类：

```bash
mvn test -Dtest=DeadLetterRedeliveryTest
mvn test -Dtest=RetryBackoffTest
```

## 已知限制

- 进程内实现，重启不持久化；订阅关系、未确认消息与死信留存仅存于内存。
- 每个订阅的投递状态表在订阅生命周期内保留终态记录（用于区分重复/迟到确认与诊断），
  取消订阅时整体释放；死信留存为全 broker 共享、受 `dead-letter-capacity` 有界约束。
