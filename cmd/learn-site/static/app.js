const stageMeta = {
  idle: {
    headline: "准备开始",
    detail: "先创建一轮真实 demo job，再用按钮把视角推进到 instance 和 dispatch。",
  },
  job_created: {
    headline: "CreateJob 已写入",
    detail: "Control 已经把任务定义送到 leader。现在真正值得等的，是 leader create loop 补出 instance。",
  },
  instance_created: {
    headline: "Instance 已 materialize",
    detail: "这说明 cron slot 已经变成真实 instance，后续 dispatch 就有对象可以推进了。",
  },
  dispatched: {
    headline: "Dispatch 已发出",
    detail: "Master 已经先写库再发 RPC。这个阶段最容易看到“数据库先行，网络副作用后置”的设计。",
  },
  running: {
    headline: "Worker 已 started",
    detail: "started 成功以后，instance 会进入 running，接着我们会等待 heartbeat 出现。",
  },
  heartbeat_seen: {
    headline: "Heartbeat 已出现",
    detail: "这里最能证明任务真在跑，而不是卡在 dispatched。heartbeat 是一条很好的实时可信信号。",
  },
  finished: {
    headline: "Finished 已回调",
    detail: "业务执行已经收敛回 MySQL，接下来是 relay 和 audit 追上状态。",
  },
  outbox_sent: {
    headline: "Outbox 已送出",
    detail: "主事务和 outbox 已经闭环，消息侧正在接棒把事件扩散出去。",
  },
  audit_received: {
    headline: "Audit 已落库",
    detail: "这代表正常主链路完整闭环。你已经看到了 create、dispatch、execute、relay 和 audit 的真实联动。",
  },
  aborted: {
    headline: "主线已中止",
    detail: "你触发了手动 kill。后面的状态会沿 killed / failed 路径收敛，但当前教学任务就此打断。",
  },
};

const sourceLibrary = {
  "control.create_job": {
    title: "Control -> CreateJob",
    path: "cmd/control/main.go",
    summary: "教学站的“开始任务”本质上复用了现有 CreateJob RPC 路径，只是换成了网页按钮。",
    code: `leader, client := resolveLeaderClient(cfg, logger)
resp, err := client.CreateJob(actionCtx, leader.GRPCAddr, &workerpb.CreateJobRequest{
    Name:                name,
    CronExpr:            cronExpr,
    Timezone:            timezone,
    Payload:             []byte(payload),
    TimeoutSeconds:      uint32(timeoutSeconds),
    MaxRetries:          uint32(maxRetries),
    RetryBackoffSeconds: uint32(retryBackoff),
    AllowConcurrent:     allowConcurrent,
    Status:              status,
})`,
  },
  "master.materialize_due_instances": {
    title: "Master -> MaterializeDueInstances",
    path: "internal/app/master/create.go",
    summary: "真正把 cron slot 变成 instance 的，是 leader 的 create loop，而不是 CreateJob RPC 本身。",
    code: `windowStart := now.Add(-s.cfg.Scheduling.Lookback).UTC()
windowEnd := now.Add(s.cfg.Scheduling.Lookahead).UTC()

id, err := tx.Instances().Create(ctx, instance)
...
outboxID, err = s.enqueueLifecycleEvent(ctx, tx, envelope, instanceEventKey(current.ID), headers)`,
  },
  "master.dispatch": {
    title: "Master -> DispatchPending",
    path: "internal/app/master/dispatch.go",
    summary: "dispatch 的核心是先写 instance / attempt / outbox，再发 DispatchTask RPC。",
    code: `ok, err := tx.Instances().MarkDispatched(ctx, instanceID, selected.WorkerID, nextAttemptNo)
...
attemptID, err = tx.Attempts().Create(ctx, attempt)
...
_, rpcErr := s.workerClient.DispatchTask(rpcCtx, selected.GRPCAddr, &workerpb.DispatchTaskRequest{
    WorkerId:   selected.WorkerID,
    InstanceId: instanceID,
    AttemptNo:  nextAttemptNo,
})`,
  },
  "worker.dispatch_task": {
    title: "Worker -> DispatchTask",
    path: "internal/app/worker/execute.go",
    summary: "Worker 先校验 worker_id，再建立 execution handle，随后反复上报 started 直到成功。",
    code: `func (s *Service) DispatchTask(ctx context.Context, req *workerpb.DispatchTaskRequest) (*workerpb.DispatchTaskResponse, error) {
    if req.WorkerId != s.cfg.App.ID {
        return &workerpb.DispatchTaskResponse{Accepted: false, Message: "worker id mismatch"}, nil
    }
    if err := s.reportStartedUntilSuccess(execCtx, req.InstanceId, req.AttemptNo, startedAt); err != nil {
        return
    }
}`,
  },
  "worker.heartbeat": {
    title: "Worker -> Heartbeat",
    path: "internal/app/worker/heartbeat.go",
    summary: "heartbeat loop 会持续 leader-aware 上报 last_heartbeat_at，这就是舞台上 heartbeat 节点脉冲的来源。",
    code: `_, err = s.masterClient.ReportHeartbeat(callCtx, leader.GRPCAddr, &workerpb.ReportHeartbeatRequest{
    InstanceId:        handle.instanceID,
    AttemptNo:         handle.attemptNo,
    HeartbeatAtUnixMs: now.UnixMilli(),
})`,
  },
  "master.report_finished": {
    title: "Master -> ReportFinished",
    path: "internal/app/master/rpc.go / internal/app/master/dispatch.go",
    summary: "finished 回调进入 master 后，会配合 attempt_no fencing 决定是否真的改写 instance / attempt 终态。",
    code: `func (s *Service) ReportFinished(ctx context.Context, req *workerpb.ReportFinishedRequest) (*workerpb.ReportFinishedResponse, error) {
    if err := s.reportFinished(ctx, req.InstanceId, req.AttemptNo, req.Status, unixMS(req.FinishedAtUnixMs), int(req.ExitCode), req.ErrorMessage, req.ResultSummary); err != nil {
        return nil, err
    }
    return &workerpb.ReportFinishedResponse{Accepted: true}, nil
}`,
  },
  "master.outbox_relay": {
    title: "Master -> relayPendingOutbox",
    path: "internal/app/master/events.go",
    summary: "业务状态先完成，outbox 后台再发消息；这就是教学站里 MySQL -> Kafka 的后半程动画。",
    code: `events, err := s.store.Outbox().ListPending(ctx, s.now().UTC(), s.cfg.Messaging.RelayBatchSize)
...
publishErr := s.producer.Publish(ctx, messaginginfra.Record{
    Topic:   event.Topic,
    Key:     event.EventKey,
    Value:   event.Payload,
    Headers: headers,
})
...
_, err := s.store.Outbox().MarkSent(ctx, event.ID, s.now().UTC())`,
  },
  "audit.consumer": {
    title: "Audit Consumer -> audit_events",
    path: "cmd/audit-consumer/main.go / internal/repository/mysql/audit_repo.go",
    summary: "audit consumer 负责把生命周期事件幂等落到 audit_events，页面上的 Audit 亮起就对应这一步。",
    code: `INSERT INTO audit_events (
    event_id,
    event_type,
    aggregate_type,
    aggregate_id,
    instance_id,
    attempt_no,
    job_id,
    worker_id,
    trace_id,
    payload,
    received_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  },
  "master.kill_instance": {
    title: "Master -> KillInstance",
    path: "internal/app/master/rpc.go",
    summary: "实验动作里的 Kill 当前实例，本质上仍然是 leader-aware 的 KillInstance RPC。",
    code: `func (s *Service) KillInstance(ctx context.Context, req *workerpb.KillInstanceRequest) (*workerpb.KillInstanceResponse, error) {
    reason := strings.TrimSpace(req.Reason)
    if reason == "" {
        reason = "manual kill"
    }
    if err := s.killInstanceByID(ctx, req.InstanceId, reason); err != nil {
        return nil, err
    }
    return &workerpb.KillInstanceResponse{Accepted: true, Message: "kill requested"}, nil
}`,
  },
  "local.master": {
    title: "Local Cluster -> Master Replica",
    path: "cmd/master/main.go",
    summary: "学习站现在会用显式端口和实例 ID 启动多个 master 副本；它们会共同参与 etcd 选主，但始终只有一个 active leader。",
    code: `go run ./cmd/master -config configs/local.yaml -id "master-local-b" -listen "127.0.0.1:8081" -advertise "127.0.0.1:8081" -http-listen "127.0.0.1:18081"`,
  },
  "local.worker": {
    title: "Local Cluster -> Worker Replica",
    path: "cmd/worker/main.go",
    summary: "学习站会用不同端口启动多个 worker 副本；它们注册进 etcd 后，master 就能按负载把 instance 分散派发过去。",
    code: `go run ./cmd/worker -config configs/local.yaml -id "worker-local-b" -listen "127.0.0.1:9091" -advertise "127.0.0.1:9091" -http-listen "127.0.0.1:19081"`,
  },
};

["node.control", "button.start", "button.refresh"].forEach((key) => {
  sourceLibrary[key] = sourceLibrary["control.create_job"];
});
["node.master", "button.focus", "button.dispatch"].forEach((key) => {
  sourceLibrary[key] = sourceLibrary["master.dispatch"];
});
["button.await", "slot.window", "node.mysql"].forEach((key) => {
  sourceLibrary[key] = sourceLibrary["master.materialize_due_instances"];
});
sourceLibrary["node.worker"] = sourceLibrary["worker.dispatch_task"];
sourceLibrary["node.kafka"] = sourceLibrary["master.outbox_relay"];
sourceLibrary["node.audit"] = sourceLibrary["audit.consumer"];
sourceLibrary["button.kill"] = sourceLibrary["master.kill_instance"];
sourceLibrary["button.master"] = sourceLibrary["local.master"];
sourceLibrary["button.worker"] = sourceLibrary["local.worker"];

const actorIds = ["control", "master", "worker", "mysql", "kafka", "audit"];
const orderedStages = [
  "idle",
  "job_created",
  "instance_created",
  "dispatched",
  "running",
  "heartbeat_seen",
  "finished",
  "outbox_sent",
  "audit_received",
  "aborted",
];
const stageToActors = {
  idle: [],
  job_created: ["control", "master", "mysql"],
  instance_created: ["master", "mysql"],
  dispatched: ["master", "worker", "mysql"],
  running: ["master", "worker", "mysql"],
  heartbeat_seen: ["worker", "master"],
  finished: ["worker", "master", "mysql"],
  outbox_sent: ["mysql", "kafka"],
  audit_received: ["kafka", "audit"],
  aborted: ["control", "master", "worker"],
};

const state = {
  scene: null,
  pinnedSourceKey: "",
  hoverSourceKey: "",
  eventSource: null,
  streamConnected: false,
  lastStage: "idle",
  lastHeartbeatAt: "",
  replayBusy: false,
};

const elements = {
  streamState: document.querySelector("#stream-state"),
  streamDetail: document.querySelector("#stream-detail"),
  heroStage: document.querySelector("#hero-stage"),
  heroStageDetail: document.querySelector("#hero-stage-detail"),
  heroSession: document.querySelector("#hero-session"),
  heroSessionDetail: document.querySelector("#hero-session-detail"),
  stageBadge: document.querySelector("#stage-badge"),
  leaderBadge: document.querySelector("#leader-badge"),
  dependencyStrip: document.querySelector("#dependency-strip"),
  blockerStrip: document.querySelector("#blocker-strip"),
  processGrid: document.querySelector("#process-grid"),
  runtimeStage: document.querySelector("#runtime-stage"),
  packetLayer: document.querySelector("#packet-layer"),
  summaryHeadline: document.querySelector("#summary-headline"),
  summaryText: document.querySelector("#summary-text"),
  slotWindowTitle: document.querySelector("#slot-window-title"),
  slotWindowDetail: document.querySelector("#slot-window-detail"),
  slotWindowStart: document.querySelector("#slot-window-start"),
  slotWindowNow: document.querySelector("#slot-window-now"),
  slotWindowEnd: document.querySelector("#slot-window-end"),
  slotTrack: document.querySelector("#slot-track"),
  checkpointStrip: document.querySelector("#checkpoint-strip"),
  timelineList: document.querySelector("#timeline-list"),
  missionPill: document.querySelector("#mission-pill"),
  missionMeta: document.querySelector("#mission-meta"),
  instanceList: document.querySelector("#instance-list"),
  workerList: document.querySelector("#worker-list"),
  outboxList: document.querySelector("#outbox-list"),
  auditList: document.querySelector("#audit-list"),
  sourceCard: document.querySelector("#source-card"),
  focusSelect: document.querySelector("#focus-select"),
  recentSource: document.querySelector("#recent-source"),
  recentFailures: document.querySelector("#recent-failures"),
  startButtons: [
    document.querySelector("#hero-start"),
    document.querySelector("#control-start"),
  ],
  awaitButtons: [
    document.querySelector("#hero-await-instance"),
    document.querySelector("#control-await-instance"),
  ],
  dispatchButtons: [
    document.querySelector("#hero-advance-dispatch"),
    document.querySelector("#control-advance-dispatch"),
  ],
  refreshButtons: [
    document.querySelector("#hero-refresh"),
    document.querySelector("#control-refresh"),
  ],
  replayButton: document.querySelector("#control-replay"),
  focusButton: document.querySelector("#focus-apply"),
  killButton: document.querySelector("#kill-button"),
  recentRefresh: document.querySelector("#recent-refresh"),
};

function formatTime(raw) {
  if (!raw) {
    return "—";
  }
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) {
    return "—";
  }
  return new Intl.DateTimeFormat("zh-CN", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(date);
}

function stageIndex(stage) {
  const index = orderedStages.indexOf(stage);
  return index === -1 ? 0 : index;
}

function escapeHtml(text) {
  return String(text ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

async function fetchJSON(url, options = {}) {
  const response = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.error || `Request failed: ${response.status}`);
  }
  return data;
}

function setStreamState(connected, detail) {
  state.streamConnected = connected;
  elements.streamState.textContent = connected ? "live" : "offline";
  elements.streamDetail.textContent = detail;
}

function sourceEntry(key) {
  return (
    sourceLibrary[key] ||
    sourceLibrary[state.scene?.sourceKey] ||
    sourceLibrary["control.create_job"]
  );
}

function currentSourceKey() {
  return state.pinnedSourceKey || state.hoverSourceKey || state.scene?.sourceKey || "control.create_job";
}

function renderSourceCard() {
  const key = currentSourceKey();
  const entry = sourceEntry(key);
  elements.sourceCard.classList.toggle("pinned", Boolean(state.pinnedSourceKey));
  elements.sourceCard.innerHTML = `
    <div class="source-head">
      <div>
        <p class="eyebrow">Source Lens</p>
        <h3>${escapeHtml(entry.title)}</h3>
        <p class="source-path">${escapeHtml(entry.path)}</p>
      </div>
      <span class="mission-pill ${state.pinnedSourceKey ? "" : "subtle"}">
        ${state.pinnedSourceKey ? "已固定" : "悬浮切换"}
      </span>
    </div>
    <p>${escapeHtml(entry.summary)}</p>
    <pre><code>${escapeHtml(entry.code)}</code></pre>
  `;
}

function renderDependencies(scene) {
  elements.dependencyStrip.innerHTML = scene.dependencies
    .map(
      (item) => `
        <div class="dependency-chip ${escapeHtml(item.status)}">
          <strong>${escapeHtml(item.label)}</strong>
          <span>${escapeHtml(item.detail)}</span>
        </div>
      `,
    )
    .join("");
}

function renderProcessControls(scene) {
  if (!scene.processes?.length) {
    elements.processGrid.innerHTML = `<div class="process-item"><strong>暂无本地控制按钮</strong><p>当前学习站没有可启动的本地脚本。</p></div>`;
    return;
  }

  const groups = [
    {
      key: "master",
      title: "Master Replicas",
      detail: "多个 master 会同时监听，但 etcd 只会选出一个 active leader。",
    },
    {
      key: "worker",
      title: "Worker Replicas",
      detail: "多个 worker 注册后，master 会按负载把 instance 分散到不同节点。",
    },
  ];

  elements.processGrid.innerHTML = groups
    .map((group) => {
      const items = scene.processes.filter((item) => item.kind === group.key);
      if (!items.length) {
        return "";
      }
      return `
        <section class="process-group">
          <div class="process-group-head">
            <strong>${escapeHtml(group.title)}</strong>
            <p>${escapeHtml(group.detail)}</p>
          </div>
          <div class="process-group-grid">
            ${items
              .map(
                (item) => `
                  <article class="process-item ${escapeHtml(item.kind)}">
                    <div class="process-item-head">
                      <div>
                        <span class="process-kind">${escapeHtml(item.kind)}</span>
                        <strong>${escapeHtml(item.label)}</strong>
                      </div>
                      <span class="process-status ${escapeHtml(item.status)}">${escapeHtml(item.status)}</span>
                    </div>
                    <p class="process-address">gRPC ${escapeHtml(item.listenAddr)} · HTTP ${escapeHtml(item.httpAddr)}</p>
                    <p>${escapeHtml(item.detail)}</p>
                    <button
                      type="button"
                      class="button secondary process-button ${item.running ? "is-running" : ""}"
                      data-process-id="${escapeHtml(item.id)}"
                      data-command="${escapeHtml(item.command)}"
                      data-source-key="${escapeHtml(item.sourceKey)}"
                      title="${escapeHtml(item.command)}">
                      ${item.running ? `检查 ${escapeHtml(item.label)}` : `启动 ${escapeHtml(item.label)}`}
                    </button>
                  </article>
                `,
              )
              .join("")}
          </div>
        </section>
      `;
    })
    .join("");
}

function renderBlockers(scene) {
  if (!scene.blockers?.length) {
    elements.blockerStrip.innerHTML = "";
    return;
  }
  elements.blockerStrip.innerHTML = scene.blockers
    .map((item) => `<div class="blocker-chip">${escapeHtml(item)}</div>`)
    .join("");
}

function updateLinks() {
  const stageRect = elements.runtimeStage.getBoundingClientRect();
  document.querySelectorAll(".stage-link").forEach((link) => {
    const from = document.querySelector(`[data-actor="${link.dataset.from}"]`);
    const to = document.querySelector(`[data-actor="${link.dataset.to}"]`);
    if (!from || !to) {
      return;
    }
    const fromRect = from.getBoundingClientRect();
    const toRect = to.getBoundingClientRect();
    const x1 = fromRect.left - stageRect.left + fromRect.width / 2;
    const y1 = fromRect.top - stageRect.top + fromRect.height / 2;
    const x2 = toRect.left - stageRect.left + toRect.width / 2;
    const y2 = toRect.top - stageRect.top + toRect.height / 2;
    const dx = x2 - x1;
    const dy = y2 - y1;
    const angle = (Math.atan2(dy, dx) * 180) / Math.PI;
    const length = Math.sqrt(dx * dx + dy * dy);
    link.style.left = `${x1}px`;
    link.style.top = `${y1}px`;
    link.style.width = `${length}px`;
    link.style.transform = `rotate(${angle}deg)`;
  });
}

function renderActors(scene) {
  const activeActors = new Set(stageToActors[scene.stage] || []);
  const actorMap = new Map(scene.actors.map((actor) => [actor.id, actor]));
  actorIds.forEach((id) => {
    const card = document.querySelector(`[data-actor="${id}"]`);
    const actor = actorMap.get(id);
    if (!card || !actor) {
      return;
    }
    card.classList.toggle("is-active", activeActors.has(id) || actor.active);
    card.querySelector(`[data-role-status="${id}"]`).textContent = actor.status;
    card.querySelector(`[data-role-detail="${id}"]`).textContent = actor.detail;
  });

  const activeLinks = new Set();
  (scene.packets || []).forEach((packet) => {
    for (let i = 0; i < packet.route.length - 1; i += 1) {
      activeLinks.add(`${packet.route[i]}-${packet.route[i + 1]}`);
      activeLinks.add(`${packet.route[i + 1]}-${packet.route[i]}`);
    }
  });
  document.querySelectorAll(".stage-link").forEach((link) => {
    const key = `${link.dataset.from}-${link.dataset.to}`;
    link.classList.toggle("is-active", activeLinks.has(key));
  });
}

function renderCheckpoints(scene) {
  elements.checkpointStrip.innerHTML = scene.checkpoints
    .map(
      (checkpoint) => `
        <article
          class="checkpoint-card ${escapeHtml(checkpoint.state)}"
          data-source-key="${escapeHtml(checkpoint.sourceKey)}"
          data-pin-source="true">
          <span class="label">${escapeHtml(checkpoint.state)}</span>
          <strong>${escapeHtml(checkpoint.label)}</strong>
          <p>${escapeHtml(checkpoint.description)}</p>
        </article>
      `,
    )
    .join("");
}

function renderTimeline(scene) {
  if (!scene.timeline?.length) {
    elements.timelineList.innerHTML = `<div class="timeline-item"><strong>时间线还在等待真实事件</strong><p>现在先盯着上面的舞台和依赖状态。</p></div>`;
    return;
  }
  elements.timelineList.innerHTML = scene.timeline
    .map(
      (item) => `
        <article
          class="timeline-item"
          data-source-key="${escapeHtml(item.sourceKey)}"
          data-pin-source="true">
          <time>${escapeHtml(formatTime(item.occurredAt))}</time>
          <strong>${escapeHtml(item.label)}</strong>
          <p>${escapeHtml(item.detail)}</p>
        </article>
      `,
    )
    .join("");
}

function renderMission(scene) {
  elements.missionPill.textContent = scene.session.status;
  const createdAt = scene.session.createdAt ? formatTime(scene.session.createdAt) : "—";
  elements.missionMeta.innerHTML = `
    <div class="meta-row"><span>session</span><strong>${escapeHtml(scene.session.id || "暂无")}</strong></div>
    <div class="meta-row"><span>job</span><strong>${scene.tracked.jobId ? `#${scene.tracked.jobId}` : "—"}</strong></div>
    <div class="meta-row"><span>instance</span><strong>${scene.tracked.instanceId ? `#${scene.tracked.instanceId}` : "—"}</strong></div>
    <div class="meta-row"><span>attempt</span><strong>${scene.tracked.attemptNo || "—"}</strong></div>
    <div class="meta-row"><span>leader</span><strong>${escapeHtml(scene.tracked.leaderId || "—")}</strong></div>
    <div class="meta-row"><span>started at</span><strong>${escapeHtml(createdAt)}</strong></div>
  `;

  elements.instanceList.innerHTML = scene.tracked.instances?.length
    ? scene.tracked.instances
        .map(
          (item) => `
            <button
              type="button"
              class="tiny-chip ${item.focused ? "focused" : ""}"
              data-instance-chip="${item.id}"
              data-source-key="master.dispatch"
              title="instance #${item.id}">
              #${item.id} · ${escapeHtml(item.status)}
            </button>
          `,
        )
        .join("")
    : `<span class="tiny-chip">等待 instance</span>`;

  elements.workerList.innerHTML = scene.tracked.availableWorkers?.length
    ? scene.tracked.availableWorkers
        .map(
          (item) => `
            <span class="tiny-chip ${item.primary ? "focused" : ""}">
              ${escapeHtml(item.id)} · ${item.online ? "online" : "offline"}
            </span>
          `,
        )
        .join("")
    : `<span class="tiny-chip">暂无 worker</span>`;

  elements.outboxList.innerHTML = scene.tracked.outbox?.length
    ? scene.tracked.outbox
        .map(
          (item) => `
            <div class="mini-item" data-source-key="master.outbox_relay" data-pin-source="true">
              <strong>${escapeHtml(item.eventType)}</strong>
              <p>${escapeHtml(item.status)} · ${escapeHtml(formatTime(item.sentAt || item.createdAt))}</p>
            </div>
          `,
        )
        .join("")
    : `<div class="mini-item"><strong>暂无 outbox</strong><p>等待 dispatch / finished 事件出现。</p></div>`;

  elements.auditList.innerHTML = scene.tracked.audit?.length
    ? scene.tracked.audit
        .map(
          (item) => `
            <div class="mini-item" data-source-key="audit.consumer" data-pin-source="true">
              <strong>${escapeHtml(item.eventType)}</strong>
              <p>${escapeHtml(formatTime(item.receivedAt))}</p>
            </div>
          `,
        )
        .join("")
    : `<div class="mini-item"><strong>暂无 audit</strong><p>等待 consumer 消费 lifecycle 事件。</p></div>`;
}

function renderFocusSelect(scene) {
  const instances = scene.tracked.instances || [];
  if (!instances.length) {
    elements.focusSelect.innerHTML = `<option value="">等待 instance</option>`;
    elements.focusSelect.disabled = true;
    elements.focusButton.disabled = true;
    return;
  }
  elements.focusSelect.disabled = false;
  elements.focusButton.disabled = false;
  elements.focusSelect.innerHTML = instances
    .map(
      (item) => `
        <option value="${item.id}" ${item.focused ? "selected" : ""}>
          instance #${item.id} · ${escapeHtml(item.status)}
        </option>
      `,
    )
    .join("");
}

function renderHero(scene) {
  const meta = stageMeta[scene.stage] || stageMeta.idle;
  elements.heroStage.textContent = scene.stage;
  elements.heroStageDetail.textContent = meta.detail;
  elements.summaryHeadline.textContent = meta.headline;
  elements.summaryText.textContent = scene.summary || meta.detail;
  elements.heroSession.textContent = scene.session.id || "暂无会话";
  elements.heroSessionDetail.textContent = scene.session.jobName
    ? `${scene.session.jobName} · 创建于 ${formatTime(scene.session.createdAt)}`
    : "点击“创建 Job”后会生成新的教学任务。";
  elements.stageBadge.textContent = scene.stage;
  elements.leaderBadge.textContent = scene.tracked.leaderId
    ? `leader ${scene.tracked.leaderId}`
    : "leader unknown";
}

function renderSlots(scene) {
  const slots = scene.slots || {};
  elements.slotWindowTitle.textContent = slots.available
    ? `${slots.cronExpr} · ${slots.timezone}`
    : "等待 Job";
  elements.slotWindowDetail.textContent = slots.detail || "创建 Job 后，这里会显示真实 cron 时间槽。";
  elements.slotWindowStart.textContent = formatTime(slots.windowStart);
  elements.slotWindowNow.textContent = formatTime(slots.cursorAt || scene.generatedAt);
  elements.slotWindowEnd.textContent = formatTime(slots.windowEnd);

  if (!slots.available || !slots.slots?.length) {
    elements.slotTrack.innerHTML = `
      <div class="slot-empty">
        <div>
          <strong>还没有可以展示的时间槽</strong>
          <p>先创建一轮 demo job，leader 的 create window 才会在这里出现。</p>
        </div>
      </div>
    `;
    return;
  }

  const startMs = new Date(slots.windowStart).getTime();
  const endMs = new Date(slots.windowEnd).getTime();
  const cursorMs = new Date(slots.cursorAt || scene.generatedAt).getTime();
  const widthMs = Math.max(endMs - startMs, 1);
  const nowPct = Math.min(Math.max(((cursorMs - startMs) / widthMs) * 100, 0), 100);

  const nodes = slots.slots
    .map((slot) => {
      const slotMs = new Date(slot.scheduledAt).getTime();
      const left = Math.min(Math.max(((slotMs - startMs) / widthMs) * 100, 0), 100);
      const subtitle = slot.instanceId
        ? `instance #${slot.instanceId}${slot.instanceStatus ? ` · ${slot.instanceStatus}` : ""}`
        : slot.state === "future"
          ? "future slot"
          : "due slot";
      return `
        <article
          class="slot-node ${escapeHtml(slot.state)}"
          style="left: ${left}%"
          data-source-key="master.materialize_due_instances"
          data-pin-source="true">
          <div class="slot-node-card">
            <strong class="slot-time">${escapeHtml(slot.label)}</strong>
            <strong class="slot-subtitle">${escapeHtml(subtitle)}</strong>
            <p>${escapeHtml(slot.detail)}</p>
          </div>
        </article>
      `;
    })
    .join("");

  elements.slotTrack.innerHTML = `
    <div class="slot-now-marker" style="left: ${nowPct}%"><span>now</span></div>
    ${nodes}
  `;
}

function updateActionButtons(scene) {
  const hasSession = Boolean(scene.session?.id);
  const instanceReached = stageIndex(scene.stage) >= stageIndex("instance_created");
  const dispatchReached = stageIndex(scene.stage) >= stageIndex("dispatched");

  elements.awaitButtons.forEach((button) => {
    if (!button) {
      return;
    }
    button.disabled = !hasSession || instanceReached;
  });

  elements.dispatchButtons.forEach((button) => {
    if (!button) {
      return;
    }
    button.disabled = !hasSession || dispatchReached;
  });
}

function renderRecentFailures(data) {
  elements.recentSource.textContent = data.source || "waiting";
  if (!data.instances?.length) {
    elements.recentFailures.innerHTML = `<div class="mini-item"><strong>暂无 recent failures</strong><p>如果这里一直为空，说明最近没有失败实例。</p></div>`;
    return;
  }
  elements.recentFailures.innerHTML = data.instances
    .map(
      (item) => `
        <div class="mini-item">
          <strong>#${item.id}</strong>
          <p>${escapeHtml(item.status)} · ${escapeHtml(formatTime(item.finishedAt || item.scheduledAt))}</p>
        </div>
      `,
    )
    .join("");
}

function actorCenter(actorId) {
  const actor = document.querySelector(`[data-actor="${actorId}"]`);
  const stageRect = elements.runtimeStage.getBoundingClientRect();
  const rect = actor.getBoundingClientRect();
  return {
    x: rect.left - stageRect.left + rect.width / 2,
    y: rect.top - stageRect.top + rect.height / 2,
  };
}

function pulseActors(route) {
  const uniqueIds = [...new Set(route)];
  uniqueIds.forEach((id) => {
    const actor = document.querySelector(`[data-actor="${id}"]`);
    actor?.classList.remove("is-pulsing");
    void actor?.offsetWidth;
    actor?.classList.add("is-pulsing");
    window.setTimeout(() => actor?.classList.remove("is-pulsing"), 900);
  });
}

async function animatePacket(packet) {
  const bubble = document.createElement("div");
  bubble.className = `packet-bubble ${packet.emphasis}`;
  bubble.textContent = packet.label;
  elements.packetLayer.appendChild(bubble);
  await new Promise((resolve) => window.requestAnimationFrame(resolve));
  const width = bubble.offsetWidth;
  const height = bubble.offsetHeight;
  const route = packet.route.map(actorCenter);
  let previous = route[0];
  bubble.style.transform = `translate(${previous.x - width / 2}px, ${previous.y - height / 2}px) scale(0.92)`;
  for (let index = 1; index < route.length; index += 1) {
    const next = route[index];
    await bubble
      .animate(
        [
          { transform: `translate(${previous.x - width / 2}px, ${previous.y - height / 2}px) scale(0.92)` },
          { transform: `translate(${next.x - width / 2}px, ${next.y - height / 2}px) scale(1)` },
        ],
        {
          duration: 760,
          easing: "cubic-bezier(0.18, 0.82, 0.24, 1)",
          fill: "forwards",
        },
      )
      .finished.catch(() => undefined);
    previous = next;
  }
  window.setTimeout(() => bubble.remove(), 500);
}

async function runStageAnimation(scene, { force = false } = {}) {
  if (!scene) {
    return;
  }
  const stageChanged = state.lastStage !== scene.stage;
  const heartbeat = scene.tracked.lastHeartbeatAt || "";
  const heartbeatChanged = scene.stage === "heartbeat_seen" && heartbeat && heartbeat !== state.lastHeartbeatAt;
  if (!force && !stageChanged && !heartbeatChanged) {
    return;
  }
  if (state.replayBusy) {
    return;
  }
  state.replayBusy = true;
  pulseActors(stageToActors[scene.stage] || []);
  for (const packet of scene.packets || []) {
    // eslint-disable-next-line no-await-in-loop
    await animatePacket(packet);
  }
  state.replayBusy = false;
}

function renderScene(scene, { forceReplay = false } = {}) {
  const previousStage = state.lastStage;
  const previousHeartbeat = state.lastHeartbeatAt;
  state.scene = scene;
  renderHero(scene);
  renderDependencies(scene);
  renderProcessControls(scene);
  renderBlockers(scene);
  renderActors(scene);
  renderSlots(scene);
  renderCheckpoints(scene);
  renderTimeline(scene);
  renderMission(scene);
  renderFocusSelect(scene);
  updateActionButtons(scene);
  renderSourceCard();
  updateLinks();
  state.lastStage = scene.stage;
  state.lastHeartbeatAt = scene.tracked.lastHeartbeatAt || "";
  runStageAnimation(scene, {
    force: forceReplay || previousStage !== scene.stage || previousHeartbeat !== state.lastHeartbeatAt,
  });
}

async function refreshScene() {
  const scene = await fetchJSON("/api/runtime/scene");
  renderScene(scene);
}

async function startDemo() {
  const scene = await fetchJSON("/api/demo/start", { method: "POST" });
  renderScene(scene);
}

async function awaitInstance() {
  const scene = await fetchJSON("/api/demo/await-instance", { method: "POST" });
  renderScene(scene);
}

async function advanceToDispatch() {
  const scene = await fetchJSON("/api/demo/advance-dispatch", { method: "POST" });
  renderScene(scene);
}

async function focusInstance(instanceId) {
  const scene = await fetchJSON("/api/demo/focus", {
    method: "POST",
    body: JSON.stringify({ instanceId }),
  });
  renderScene(scene);
}

async function killTracked() {
  const scene = await fetchJSON("/api/demo/kill", { method: "POST" });
  renderScene(scene);
}

async function refreshRecentFailures() {
  const data = await fetchJSON("/api/demo/recent-failures?limit=6");
  renderRecentFailures(data);
}

async function startLocalProcess(id) {
  const scene = await fetchJSON("/api/local/processes/start", {
    method: "POST",
    body: JSON.stringify({ id }),
  });
  renderScene(scene);
}

function connectStream() {
  if (state.eventSource) {
    state.eventSource.close();
  }
  const source = new EventSource("/api/runtime/stream");
  state.eventSource = source;
  setStreamState(false, "正在连接 runtime stream…");
  source.addEventListener("open", () => {
    setStreamState(true, "runtime stream 已连接，每秒刷新一次真实快照。");
  });
  source.addEventListener("scene", (event) => {
    const scene = JSON.parse(event.data);
    renderScene(scene);
  });
  source.addEventListener("error", () => {
    setStreamState(false, "runtime stream 已断开，3 秒后会自动重连。");
    source.close();
    window.setTimeout(connectStream, 3000);
  });
}

function bindButtons() {
  elements.startButtons.forEach((button) => {
    button?.addEventListener("click", async () => {
      try {
        await startDemo();
      } catch (error) {
        window.alert(error.message);
      }
    });
  });

  elements.awaitButtons.forEach((button) => {
    button?.addEventListener("click", async () => {
      try {
        await awaitInstance();
      } catch (error) {
        window.alert(error.message);
      }
    });
  });

  elements.dispatchButtons.forEach((button) => {
    button?.addEventListener("click", async () => {
      try {
        await advanceToDispatch();
      } catch (error) {
        window.alert(error.message);
      }
    });
  });

  elements.refreshButtons.forEach((button) => {
    button?.addEventListener("click", async () => {
      try {
        await refreshScene();
      } catch (error) {
        window.alert(error.message);
      }
    });
  });

  elements.replayButton?.addEventListener("click", () => {
    renderScene(state.scene, { forceReplay: true });
  });

  elements.focusButton?.addEventListener("click", async () => {
    const value = Number(elements.focusSelect.value);
    if (!value) {
      return;
    }
    try {
      await focusInstance(value);
    } catch (error) {
      window.alert(error.message);
    }
  });

  elements.killButton?.addEventListener("click", async () => {
    try {
      await killTracked();
    } catch (error) {
      window.alert(error.message);
    }
  });

  elements.recentRefresh?.addEventListener("click", async () => {
    try {
      await refreshRecentFailures();
    } catch (error) {
      window.alert(error.message);
    }
  });

  elements.instanceList?.addEventListener("click", async (event) => {
    const chip = event.target.closest("[data-instance-chip]");
    if (!chip) {
      return;
    }
    const value = Number(chip.dataset.instanceChip);
    elements.focusSelect.value = String(value);
    try {
      await focusInstance(value);
    } catch (error) {
      window.alert(error.message);
    }
  });

  elements.processGrid?.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-process-id]");
    if (!button) {
      return;
    }
    try {
      await startLocalProcess(button.dataset.processId);
    } catch (error) {
      window.alert(error.message);
    }
  });
}

function bindSourceInteractions() {
  document.addEventListener("mouseover", (event) => {
    const target = event.target.closest("[data-source-key]");
    if (!target || state.pinnedSourceKey) {
      return;
    }
    state.hoverSourceKey = target.dataset.sourceKey;
    renderSourceCard();
  });

  document.addEventListener("mouseout", (event) => {
    const target = event.target.closest("[data-source-key]");
    if (!target || state.pinnedSourceKey) {
      return;
    }
    if (target.contains(event.relatedTarget)) {
      return;
    }
    state.hoverSourceKey = "";
    renderSourceCard();
  });

  document.addEventListener("click", (event) => {
    const target = event.target.closest("[data-pin-source='true']");
    document.querySelectorAll("[data-pin-source='true']").forEach((node) => node.classList.remove("is-pinned"));
    if (!target) {
      state.pinnedSourceKey = "";
      renderSourceCard();
      return;
    }
    const key = target.dataset.sourceKey;
    if (state.pinnedSourceKey === key) {
      state.pinnedSourceKey = "";
      renderSourceCard();
      return;
    }
    state.pinnedSourceKey = key;
    target.classList.add("is-pinned");
    renderSourceCard();
  });
}

function bindWindowEvents() {
  window.addEventListener("resize", updateLinks);
}

async function boot() {
  bindButtons();
  bindSourceInteractions();
  bindWindowEvents();
  renderSourceCard();
  connectStream();
  try {
    await Promise.all([refreshScene(), refreshRecentFailures()]);
  } catch (error) {
    setStreamState(false, error.message);
  }
}

boot();
