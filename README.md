# robot-topic-delivery-java

进程内机器人主题通信与可靠投递组件（Spring Boot 3 / Java 21+）。不依赖任何外部消息代理或外部服务，
提供层级主题发布订阅、可靠投递状态机、有界队列背压、连接中断与恢复补投，以及可核验的统计诊断。

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

**守恒不变量**（静默后可验证）：

```
enqueued == acked + dead + dropped        （无在途时）
```

`broker.diagnostics()` 提供每个订阅的队列深度、在途数、各状态计数与连接状态。

## 配置项（application.properties）

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `broker.queue-capacity-per-subscription` | 64 | 每订阅队列容量，满时背压拒绝 |
| `broker.ack-timeout-millis` | 500 | 确认超时，超时触发重试 |
| `broker.max-retries` | 3 | 最大重试次数，耗尽进入 DEAD |
| `broker.dispatch-interval-millis` | 5 | 派发循环空转间隔 |

## 本地验证

```bash
mvn test
```

测试覆盖（日志打印关键输入、状态迁移与判定依据）：

- `TopicMatchingTest`：精确/单层/多层通配匹配，非法主题与非法过滤器拒绝；
- `BrokerReliabilityTest`：发布校验的可区分拒绝、确认流转与计数守恒、重复/乱序/迟到确认、
  重试耗尽进入 DEAD；
- `BrokerConcurrencyTest`：8 线程并发发布至多订阅者、慢订阅者队列饱和背压（且不影响快订阅者）、
  断连期间不投递不丢失、恢复补投、在途未确认消息的恢复重投、取消订阅的确定性丢弃、
  发布过程中动态增删订阅。

## 已知限制

- 进程内实现，重启不持久化；订阅关系与未确认消息仅存于内存。
- 每个订阅的投递状态表在订阅生命周期内保留终态记录（用于区分重复/迟到确认与诊断），
  取消订阅时整体释放。
