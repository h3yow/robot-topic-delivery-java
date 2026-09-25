# 机器人主题通信与可靠投递（robot-topic-delivery-java）

纯内存、零外部依赖的本地发布/订阅与可靠投递实现。无需消息代理、数据库或真实凭据，
仅依赖 Spring Boot Web 与 Jackson；可通过单元测试或内置 HTTP 接口在本地完整验证
并发发布、确认/超时/重试、慢订阅者、订阅动态增删、连接中断恢复与队列饱和等场景。

## 1. 包结构

```
com.github.h3yow.robottopicdeliveryjava.messaging
├── TopicMatcher            层级主题语法校验与精确/通配匹配
├── MessageCodec            负载编解码抽象
├── JsonMessageCodec        Jackson JSON 实现（失败归一为 EncodingFailureException）
├── TopicDefinition         已注册主题（名称 + 负载类型 + 是否禁止 null）
├── PublishedMessage        已接受消息（含编码字节，恢复补投无需重新编码）
├── DeliveryRecord          单订阅者维度的投递状态机（CAS 迁移）
├── Subscription            订阅定义（过滤规则、容量、背压策略、处理器）
├── MessageBroker           Broker 接口（注册/订阅/发布/断开/恢复/快照/关闭）
├── InMemoryMessageBroker   纯内存实现
├── BrokerProperties        配置项（前缀 robot.messaging）
├── BrokerStats             全局单调计数器
├── BrokerSnapshot / SubscriptionSnapshot  诊断视图
└── web/BrokerController    本地验证用 HTTP 接口
```

## 2. 层级主题与匹配规则

- 层级以 `/` 分隔，层级不允许为空：`a//b`、`/a`、`a/` 均非法。
- 具体主题（注册、发布）不允许出现通配符。
- 订阅过滤规则支持：
  - 字面量：精确匹配一个层级；
  - `+`：匹配**恰好一个**层级，例如 `robot/+/status` 匹配 `robot/r1/status`，不匹配 `robot/a/b/status`；
  - `#`：只能作为**最后一个**层级，匹配**零个或多个**层级，例如 `robot/#` 匹配 `robot`、`robot/r1/status/battery`；
  - 通配符必须独占整个层级：`a/+b`、`a/b#` 非法；`a/#/b` 非法。
- 匹配在发布瞬间对订阅集合做快照：发布之后新增/移除订阅互不影响本次投递。

### 发布拒绝（原因可区分，逐条隔离）

| 回执原因前缀          | 触发条件                                       |
|----------------------|----------------------------------------------|
| `INVALID_TOPIC`      | 主题语法非法（空层级、非法通配符等）             |
| `UNKNOWN_TOPIC`      | 主题未通过 registerTopic 注册                  |
| `INVALID_PAYLOAD`    | 负载为 null 而主题禁止 null，或负载类型与注册类型不符 |
| `ENCODING_FAILURE`   | JSON 序列化失败                                |
| `ENQUEUE_REJECTED`   | 订阅者队列饱和且策略为 REJECT（含具体订阅者与水位） |

批量发布逐条处理：任何一条（或其某个订阅者）被拒绝，不影响同批其他消息、其他订阅者。
没有任何订阅者匹配时消息仍被接受（回执原因为 `NO_SUBSCRIBER`），并计入 `publishedNoSubscriber`。

## 3. 投递语义与状态流转

投递语义为 **至少一次（at-least-once）**：订阅者必须以 `messageId` 自行去重；
每次重投 `attempt` 严格递增（从 1 开始）。每个「消息 × 订阅者」对应一条 `DeliveryRecord`：

```
                 ┌──────── nack / 超时扫描 / 断开恢复（收回 + 退避后重投，attempt 不变）
                 ▼
  PENDING ──dispatch──▶ IN_FLIGHT ──ack──▶ ACKED      （终态）
                          │
                          └──dead──▶ DEAD             （终态）
```

- **确认 ack**：只有 `IN_FLIGHT` 且尝试序号与当次控制句柄一致时才被接受，`IN_FLIGHT → ACKED`。
- **重复确认**：记录已是 `ACKED` 时再次 ack，返回 false 并计入 `duplicateAcks`，不重复投递。
- **迟到/乱序确认**：记录已超时重投（旧句柄 token 不符、状态为 PENDING/IN_FLIGHT）或已终止
  （DEAD）时 ack，返回 false；DEAD 上的确认计入 `lateAcks`。状态不倒退。
- **否定确认 nack / 处理器抛异常**：`IN_FLIGHT → PENDING`，按指数退避后从**队首**重投，计入 `retries`。
- **超时**：全局扫描线程周期检查在途记录，超过 `ack-timeout-millis` 未确认的收回队首重投。
- **终止 DEAD**：`RETRIES_EXHAUSTED`（达到 max-attempts 仍未确认）、`SUBSCRIBER_REMOVED`、
  `DROPPED_OLDEST`、`QUEUE_FULL_REJECTED`、`ENCODING_FAILURE`、`CONNECTION_LOST`。
  所有迁移均为 CAS，已 ACKED 的记录永远不会被终止或重投。
- **max-attempts 语义**：处理器对单条消息最多被调用 `max-attempts` 次；最后一次尝试超时后
  不再派发，直接 DEAD(RETRIES_EXHAUSTED)。

每个订阅者拥有独立的**单线程 worker + 有界双端队列**，保证该订阅者内严格按入队顺序处理，
慢订阅者只阻塞/拖慢自己，不影响其他订阅者。重试消息以 `offerFirst` 回到队首，排在尚未发出的消息之前；
重试采用指数退避（基数约为 ackTimeout/5，上限 30s），避免忙等。

## 4. 背压（队列饱和）策略

订阅时可单独指定容量与策略，缺省取全局配置：

| 策略         | 饱和时行为                                                | 观测                                    |
|-------------|----------------------------------------------------------|----------------------------------------|
| `BLOCK`     | 发布线程在队列上阻塞，直到有空位；不丢消息                    | 发布方自然降速；无 rejected/dead 增量    |
| `REJECT`    | 立即拒绝该订阅者的入队尝试，回执 `ENQUEUE_REJECTED`          | `rejectedEnqueues` +1，记录 DEAD(QUEUE_FULL_REJECTED) |
| `DROP_OLDEST` | 淘汰队首最旧的**待投递**消息，新消息入队尾；在途消息不被淘汰 | 被淘汰记录 DEAD(DROPPED_OLDEST)，日志含双方 messageId |

重试消息走队首且承诺不丢：即使队列已满，重投也会等待空位（自旋 + sleep），不会被背压丢弃。
`BrokerSnapshot` 暴露每个订阅者的实时 `queueSize/queueCapacity` 与各项计数。

## 5. 连接中断与恢复

- `disconnect(subscriberId)`：
  1. 标记断开并记录时间，worker 不再调用处理器；
  2. 所有**在途未确认**记录立即收回为 PENDING 并回到队首（不等待超时）；
  3. 断开期间到达的新消息正常入队排队。
- `recover(subscriberId)`：清空退避，worker 立即按队首顺序补投全部未完成消息
  （之前在途的 + 断开期间排队的），**不静默丢失已承诺消息**；重复投递不超过 at-least-once 约定。
- **宽限期**：断开后超过 `disconnect-grace-millis` 仍未恢复，该订阅者所有未完成记录
  进入 DEAD(CONNECTION_LOST)；宽限期后再 recover 不会复活已终止消息。
- `unsubscribe(subscriberId)` 的确定顺序：置移除标志 → 终止全部在途/待投递记录
  （DEAD(SUBSCRIBER_REMOVED) 并计数）→ 中断 worker。因此被移除订阅者的处理器在中断后
  “补确认”必定被拒绝；已 ACKED 的消息不受影响。

## 6. 可核验的统计与日志

`GET /api/broker/snapshot`（或 `MessageBroker.snapshot()`）返回：

- 全局：`publishedAccepted / publishedRejected / publishedNoSubscriber / deliveriesEnqueued /
  deliveriesAcked / retries / deadDeliveries / duplicateAcks / lateAcks / rejectedEnqueues`，
  以及按 `DeadReason` 分组的死信计数。计数器只增不减。
- 每订阅者：`connected / queueSize / queueCapacity / enqueued / acked / retried / dead /
  duplicateAcks / rejectedEnqueues`。

日志（SLF4J）覆盖关键输入与判定：`topic registered`、`subscriber added/removed`、
`publish accepted/rejected`（含 topic、messageId、订阅者数/具体原因）、
`dispatch ... attempt=N`、`ack accepted`、`duplicate/late ack ignored`、
`ack timeout, scheduling retry`、`nack -> retry scheduled`、`delivery dead ... reason=`、
`backpressure drop-oldest/enqueue rejected`、`subscriber disconnected/recovered`。

## 7. 配置项（application.properties）

| 键                                        | 默认值  | 说明                                    |
|------------------------------------------|--------|----------------------------------------|
| `robot.messaging.queue-capacity`         | 128    | 订阅队列默认容量                         |
| `robot.messaging.backpressure-policy`    | BLOCK  | BLOCK / REJECT / DROP_OLDEST           |
| `robot.messaging.ack-timeout-millis`     | 5000   | 在途未确认超时                           |
| `robot.messaging.max-attempts`           | 5      | 单条消息最大投递次数                     |
| `robot.messaging.disconnect-grace-millis`| 30000  | 断开宽限期                               |
| `robot.messaging.timeout-scan-interval-millis` | 20 | 超时扫描周期                          |

## 8. 本地验证方法

### 8.1 运行测试（推荐，覆盖全部边界场景）

```bash
mvn test
```

32 个测试覆盖：

- `TopicMatcherTest` / `JsonMessageCodecTest`：通配匹配与编解码失败原因；
- `BrokerRejectionTest`：未知主题、非法负载、编码失败的可区分原因；批量逐条隔离；故障订阅者不影响他人；
- `AckSemanticsTest`：重复确认幂等、超时重试后确认、迟到/乱序确认拒绝、重试耗尽终止且不继续投递；
- `BackpressureTest`：BLOCK / REJECT / DROP_OLDEST 三种饱和策略与计数；
- `SubscriptionLifecycleTest`：慢订阅者重试不丢、动态订阅只收未来消息、移除订阅者确定终止且不影响他人、重复订阅拒绝；
- `ConnectionRecoveryTest`：中断后在途+排队消息补投、无错误重复、超宽限期 CONNECTION_LOST；
- `QueueSaturationTest`：饱和可观测、恢复后队列回落、计数守恒；
- `ConcurrentPublishTest`：8 发布线程 × 250 条、3 订阅者、5000 次投递，断言无丢失/无错误重复/计数守恒；
- `DiagnosticLoggingTest`：用 Logback ListAppender 断言关键输入、状态迁移与判定依据已入日志。

### 8.2 HTTP 手工验证

```bash
mvn spring-boot:run            # 启动（端口见日志，或把 server.port 固定为 8080）

# 1) 注册主题（payloadType 为全限定类名）
curl -s -XPOST localhost:8080/api/broker/topics -H 'Content-Type: application/json' \
  -d '{"topic":"robot/r1/status","payloadType":"java.lang.String","requirePayload":true}'

# 2) 订阅（autoAck=false 可观察超时重试；可指定 queueCapacity/backpressurePolicy）
curl -s -XPOST localhost:8080/api/broker/subscribe -H 'Content-Type: application/json' \
  -d '{"subscriberId":"s1","filter":"robot/+/status","queueCapacity":4,"backpressurePolicy":"REJECT","autoAck":true}'

# 3) 发布 / 批量发布
curl -s -XPOST localhost:8080/api/broker/publish -H 'Content-Type: application/json' \
  -d '{"topic":"robot/r1/status","payload":"battery=42"}'
curl -s -XPOST localhost:8080/api/broker/publish-batch -H 'Content-Type: application/json' \
  -d '[{"topic":"robot/r1/status","payload":"a"},{"topic":"robot/r1/status","payload":"b"}]'

# 4) 查看订阅者收到的内容
curl -s localhost:8080/api/broker/inbox/s1

# 5) 模拟连接中断与恢复（用 autoAck=false 的订阅者可观察补投 attempt 递增）
curl -s -XPOST localhost:8080/api/broker/subscribe/s1/disconnect
curl -s -XPOST localhost:8080/api/broker/subscribe/s1/recover

# 6) 诊断快照（全局计数 + 各订阅者队列水位）
curl -s localhost:8080/api/broker/snapshot

# 7) 移除订阅
curl -s -XDELETE localhost:8080/api/broker/subscribe/s1
```

### 8.3 在代码中直接使用

```java
BrokerProperties props = new BrokerProperties();
props.setAckTimeoutMillis(500);
try (MessageBroker broker = new InMemoryMessageBroker(props, new JsonMessageCodec())) {
    broker.registerTopic("robot/r1/status", String.class, true);
    broker.subscribe("s1", "robot/+/status", (message, control) -> {
        System.out.println(message.messageId() + " attempt=" + message.attempt() + " " + message.payload());
        control.ack();           // 显式确认；control.nack() 立即重试
    });
    broker.publish("robot/r1/status", "battery=42");
    BrokerSnapshot snapshot = broker.snapshot();
}
```

## 9. 边界与约定

- 消息 ID 由 Broker 全局单调生成（`msg-N`），同一消息在不同订阅者间共享 ID，便于跨订阅者对账。
- Broker 为进程内内存实现：`close()` 会终止全部 worker 并 join；进程崩溃后的持久性不在本实现范围内，
  但“运行期内”的中断/恢复/超时补投保证不丢已承诺消息。
