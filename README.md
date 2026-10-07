# robot-topic-delivery-java

进程内机器人主题通信与可靠投递组件（Spring Boot 3 / Java 21+）。不依赖任何外部消息代理或外部服务，
提供层级主题发布订阅、可靠投递状态机、有界队列背压、连接中断与恢复补投、**重试退避**、
**失败留存（死信）与手动重投**，以及可核验的统计诊断。

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
PENDING --(派发)--> IN_FLIGHT --(确认)--> ACKED        [终态]
                     |  ^
                     |  | ack 超时且重试未耗尽
                     |  +----------(回到 PENDING 重试)
                     |
                     +--(重试耗尽)--> DEAD             [终态]
```

- 每个订阅拥有独立的有界队列与单派发线程：同一订阅内严格按序投递，重试会阻塞后续消息（队头阻塞语义）。
- 确认通过 `Delivery.ack()` 完成，**幂等**：
  - 重复确认：计入 `duplicateAcks`，不重复投递、不改变状态；
  - 旧 attempt 的乱序/迟到确认：计入 `lateAcks`，不回退当前状态；
  - 已终止（ACKED/DEAD）或订阅已移除后的确认：计入 `lateAcks`，终态不复活。
- 监听器抛异常视为未确认，走超时重试路径。
- 总投递次数 = 1 + `maxRetries`，之后进入 `DEAD`。
- 默认重试在超时后立即发生（兼容旧行为）；配置退避后，重试之间会等待一个可逐次拉长的间隔，
  见下文「重试退避」。

## 重试退避

超时后不会立刻重打，而是先等待一段可配置、逐次变长的间隔，避免下游抖动时被连续猛冲：

- `broker.retry-backoff-initial-millis`：第一次重试前的等待（毫秒）。**默认 0 = 立即重试，
  与旧版本行为完全一致**。
- `broker.retry-backoff-multiplier`：逐次倍率。第 n 次失败后的等待为
  `initial * multiplier^(n-1)`，默认 `2.0`（指数退避）；设为 `1.0` 即固定间隔。
- `broker.retry-backoff-max-millis`：等待上限（毫秒），默认 30000；设为 0 表示不封顶。

语义保证：

- 等待通过监视器 `wait` 挂起，**不空转占 CPU**；退避只阻塞该订阅自己的单派发线程，
  **不连累其他订阅**（其他订阅照常即时收发）。
- 等待期间 `disconnect` 会**即时唤醒**（计入 `backoffInterrupted`），消息保持 `PENDING`、
  停在当前订阅，不重投、不丢失；`reconnect` 后以递增 attempt 补投恰好一次。
- 等待期间 `unsubscribe` 同样即时唤醒，消息按既有移除语义确定性地计入 `dropped`，
  不会半途投出，也不会重复。
- 监听器抛异常与 ack 超时一样走退避路径。

## 失败留存（死信）与重投

重试耗尽进入 `DEAD` 的消息不会只剩一个计数，而是完整留存，可随时查看与手动重投。

### 留存内容与查询

每条留存（`DeadLetter`）记录：消息 id、**原本所属订阅 id**、**主题名**、**原始负载字节**
（可再用 `MessageCodec.decode` 解析）、**总投递次数**、**终结时刻**、处置状态、重投来源链。

- `broker.deadLetters()`：按终结时间从老到新的不可变快照。
- `broker.deadLetter(id)`：读取单条。
- 留存有**条数上限** `broker.dead-letter-capacity`（默认 1000），超限按
  `broker.overflow-policy` 确定性取舍：
  - `DROP_OLDEST`（默认）：新记录准入并**淘汰终结时间最早**的记录；
  - `DROP_NEWEST`：新记录被拒绝保留，老记录原样保留。

### 重投语义

`broker.replayDeadLetter(id)` 返回确定性的 `ReplayResult`：

- 重投是**一次全新的投递**：生成全新消息 id，主题/负载原样复制，attempt 从 1 重新计，
  重新走完整的「确认 / 超时 / 退避 / 重试 / 终止」流程。
- **旧记录不复活**：被接受的重投使源留存从 `RETAINED` 进入 `REPLAYING`（重投消息在途），
  新投递终结（确认 / 再次终止 / 订阅移除）后进入 `REPLAYED`。
- 重投若**再次耗尽**，会产生一条全新的、可再次重投的留存（其 `originId` 指向上一条），
  而源留存停留在 `REPLAYED`。因此同一条历史留存最多被重投一次，重投链各自独立，
  不会让同一条消息重复生效，各项计数守恒仍成立。
- 重投成功同样计入 `enqueued`，后续投递、确认、终止全部复用既有计数器。

重投请求被挡下时原因彼此可区分（`ReplayRejectReason`），并各自记账：

| 挡下原因 | 含义 |
| --- | --- |
| `DEAD_LETTER_NOT_FOUND` | id 不存在（含已被容量溢出淘汰） |
| `REPLAY_IN_FLIGHT` | 该留存的重投消息仍在投递路上 |
| `ALREADY_REPLAYED` | 已经重投过且重投消息已终结 |
| `SUBSCRIPTION_NOT_FOUND` | 原本所属订阅已不存在 |
| `QUEUE_FULL` | 重投那一刻目标订阅队列已满 |
| `BROKER_CLOSED` | broker 已关闭 |

其中撞队列满时**沿用发布背压的确定性拒绝**：计入 `enqueueRejected`，留存回滚为 `RETAINED`
（不会被标记成已重投，也不被静默吞掉），队列腾开后可再次重投。

## 背压与拒绝策略

- 每个订阅的队列容量为 `broker.queue-capacity-per-subscription`。队列满时，发布对该订阅返回
  `EnqueueResult(enqueued=false)` 并计入 `enqueueRejected`，**不影响其他订阅**——慢订阅者不会拖垮快订阅者。
- 发布本身是同步、原子的：主题/负载/编码校验失败则整体拒绝（`PublishReceipt.accepted=false`，
  原因见 `rejectReason`），校验通过则一定接受（`accepted=true`），各订阅的入队结果逐项可查。
- 订阅被移除（`unsubscribe`）时，其在途与在队消息被确定性地计为 `dropped`，不会半途中断产生歧义。

## 连接中断与恢复

- `disconnect(subscriptionId)`：暂停该订阅的派发；已入队与在途未确认消息全部保留，不投递、不丢失。
- `reconnect(subscriptionId)`：恢复派发并补投全部未确认消息（在途消息以递增的 attempt 重投），
  补投数量计入 `redeliveredOnReconnect`。重复投递只会以"同一消息的新一轮 attempt"形式出现，
  确认仍然幂等，不会产生重复生效。

## 统计与诊断

`broker.stats().snapshot()` 提供单调递增计数：

| 计数器 | 含义 |
| --- | --- |
| `published` / `publishRejected` | 发布被接受 / 被拒绝 |
| `enqueued` / `enqueueRejected` | 入队成功 / 背压拒绝 |
| `delivered` | 实际投递次数（含重投） |
| `acked` / `duplicateAcks` / `lateAcks` | 有效确认 / 重复确认 / 迟到确认 |
| `timeouts` / `retries` / `dead` | 确认超时 / 重试 / 终止 |
| `dropped` | 订阅移除时丢弃的在途消息 |
| `redeliveredOnReconnect` | 恢复时承诺补投的在途消息数 |
| `deadLetterRetained` | 当前仍在留存中的成功保留条数（DROP_OLDEST 被挤出的不计此项） |
| `deadLetterEvictedOldest` | `DROP_OLDEST` 下被淘汰的最老记录数 |
| `deadLetterEvictedNewest` | `DROP_NEWEST` 下被拒绝保留的新记录数 |
| `replayAccepted` | 被接受的重投次数（同时计入 `enqueued`） |
| `replayRejectedNotFound` / `replayRejectedInFlight` / `replayRejectedAlreadyReplayed` / `replayRejectedSubscriptionNotFound` / `replayRejectedQueueFull` | 各类重投挡下原因的次数 |
| `backoffWaits` / `backoffInterrupted` | 进入正退避等待的次数 / 被断连或移除提前唤醒的次数 |

**守恒不变量**（静默后可验证）：

```
enqueued == acked + dead + dropped        （无在途时）
```

**留存容量记账**（仅终止消息，与重投无关）：每条进入 `DEAD` 的消息必然落入三类之一——
当前仍留存（计入 `deadLetterRetained`）、被 `DROP_OLDEST` 挤出（`deadLetterEvictedOldest`）、
被 `DROP_NEWEST` 拒留（`deadLetterEvictedNewest`）。故：

```
dead == deadLetterRetained + deadLetterEvictedOldest + deadLetterEvictedNewest
```

且任意时刻留存条数（`deadLetters().size()`，含 REPLAYED 审计记录）永不超过配置容量。

`broker.diagnostics()` 提供每个订阅的队列深度、在途数、各状态计数与连接状态；
`broker.deadLetterDiagnostics()` 提供留存总数、容量与各处置状态（RETAINED/REPLAYING/REPLAYED）数量。

## 配置项（application.properties）

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `broker.queue-capacity-per-subscription` | 64 | 每订阅队列容量，满时背压拒绝 |
| `broker.ack-timeout-millis` | 500 | 确认超时，超时触发重试 |
| `broker.max-retries` | 3 | 最大重试次数，耗尽进入 DEAD |
| `broker.dispatch-interval-millis` | 5 | 派发循环空转间隔 |
| `broker.dead-letter-capacity` | 1000 | 失败留存最大条数 |
| `broker.overflow-policy` | DROP_OLDEST | 留存超限策略：`DROP_OLDEST` / `DROP_NEWEST` |
| `broker.retry-backoff-initial-millis` | 0 | 首次重试退避（毫秒），0=立即重试 |
| `broker.retry-backoff-multiplier` | 2.0 | 退避逐次倍率（1.0=固定间隔） |
| `broker.retry-backoff-max-millis` | 30000 | 退避上限（毫秒），0=不封顶 |

## 与既有语义的兼容范围

**保持不变：**

- 主题匹配、发布校验、队列背压、ack 幂等（重复/乱序/迟到确认）、断连恢复补投、
  取消订阅确定性丢弃等行为与计数全部不变。
- 退避**默认关闭**（`retry-backoff-initial-millis=0`），不配新项时重试仍是超时后立即进行，
  原有时序与断言不受影响。
- 状态机仍为 `PENDING -> IN_FLIGHT -> ACKED / DEAD`，新能力只在 DEAD 之后与重试等待之间扩展。

**会受影响：**

- 配置了正退避后，重试不再立即发生，端到端确认/终止耗时会包含退避等待。
- DEAD 终态任务现持续保留在订阅的投递状态表中（此前在留存落盘后即移除），
  与 ACKED 终态保留方式一致，取消订阅时整体释放；这修复了「DEAD 与留存落盘」之间的竞态窗口。
- 新增计数器为纯增量，`snapshot()` 只新增键、不改既有键；`diagnostics()` 结构不变，
  另增 `deadLetterDiagnostics()`。

## 本地验证

```bash
# 全量单测（28 个用例：主题匹配 5 / 可靠投递 5 / 并发 6 / 失败留存与退避 11 / 上下文加载 1）
mvn test

# 只跑新增能力
mvn test -Dtest=DeadLetterAndBackoffTest
```

Surefire 汇总输出每个用例类的用例数与通过情况；业务日志会打印每个用例的输入、
状态迁移（deliver/retry/backoff/dead/replay）与判定依据。测试覆盖：

- `TopicMatchingTest`：精确/单层/多层通配匹配，非法主题与非法过滤器拒绝；
- `BrokerReliabilityTest`：发布校验的可区分拒绝、确认流转与计数守恒、重复/乱序/迟到确认、
  重试耗尽进入 DEAD；
- `BrokerConcurrencyTest`：8 线程并发发布至多订阅者、慢订阅者队列饱和背压（且不影响快订阅者）、
  断连期间不投递不丢失、恢复补投、在途未确认消息的恢复重投、取消订阅的确定性丢弃、
  发布过程中动态增删订阅。
- `DeadLetterAndBackoffTest`：失败留存完整上下文与「留存→重投→确认」全链路、
  重投再次失败产生独立新留存而源留存不复活、五类无效/在途/已重投/订阅缺失/队列满重投被分清挡下、
  重投撞上队列饱和的确定性拒绝与腾位后可重投、退避逐次变长且不空转不连累其他订阅、
  退避期间断连停车与恢复恰好补投一次、退避期间移除确定性丢弃、
  `DROP_OLDEST`/`DROP_NEWEST` 两种容量溢出取舍与记账、零退避保持旧行为。

## 已知限制

- 进程内实现，重启不持久化；订阅关系、未确认消息与失败留存仅存于内存。
- 每个订阅的投递状态表在订阅生命周期内保留终态记录（含 DEAD，用于区分重复/迟到确认、
  消除终止与留存之间的竞态、支撑诊断），取消订阅时整体释放。
- 已进入 REPLAYED 的留存作为审计记录继续占用容量，参与 DROP_OLDEST 的淘汰次序。
