# M1 Checklist

## 文档

- [x] 已完成一页纸项目说明
- [x] 已完成术语表
- [x] 已完成四条主链路场景说明
- [x] 已完成非目标定义

## 数据模型

- [x] 已完成 `jobs / job_instances / attempts` DDL
- [x] 已定义 `uniq(job_id, scheduled_at)`
- [x] 已定义 `uniq(instance_id, attempt_no)`
- [x] 已定义关键索引

## 状态机

- [x] 已定义 instance 状态机
- [x] 已定义 attempt 状态机
- [x] 已定义非法迁移
- [x] 已定义条件更新规则
- [x] 已定义 fencing 规则

## 目录

- [x] 已完成 `cmd/internal/configs/docs/migrations/proto/scripts` 骨架
- [x] `master` 和 `worker` 空壳可编译

## 口头表达

- [ ] 能在 2 分钟内讲清项目目标和四条主链路
- [ ] 能解释为什么 `jobs / instances / attempts` 要拆开
- [ ] 能解释为什么执行层是 at-least-once
- [ ] 能解释为什么 failover 的核心是恢复调度语义

## 结论

仓库侧的 M1 交付物已经落地；最后一组口头表达项需要你自己做一次复述自检。
