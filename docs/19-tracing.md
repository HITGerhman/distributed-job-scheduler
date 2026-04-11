# Tracing

## 为什么要加

日志告诉我们发生了什么，Trace 让我们看清“同一条链路里每一步怎么串起来、哪一步最慢、哪一步失败”。

## 它解决什么

- 把 `control -> master -> worker -> master -> outbox relay -> audit consumer` 串成一条 trace
- 在 Jaeger 中定位慢点、失败点和 stale callback

## 它不解决什么

- 不替代 metrics 看全局趋势
- 不替代 Loki 看详细事件正文

## 放在系统哪里

- gRPC 通过 OTel stats handler 自动传播 context
- create / dispatch / execute / report_finished / relay_publish / audit.consume 手工补 span

## 如何验证

1. 打开 Jaeger
2. 提交一个任务
3. 观察 `control.create_job`
4. 顺着 trace 查看 `master.dispatch_pending`、`worker.execute`、`master.outbox.relay_publish`、`audit.consume`
