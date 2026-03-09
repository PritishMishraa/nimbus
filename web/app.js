const FILES = [
  {
    name: "demo-a.txt",
    contents: ["alpha", "match here", "omega"],
  },
  {
    name: "demo-b.txt",
    contents: ["skip", "another match"],
  },
];

const STANDARD_EVENTS = [
  { type: "assign", worker: "worker-a", task: "task-1" },
  { type: "complete", worker: "worker-a", task: "task-1" },
  { type: "assign", worker: "worker-b", task: "task-2" },
  { type: "complete", worker: "worker-b", task: "task-2" },
  { type: "done" },
];

const LEASE_EVENTS = [
  { type: "assign", worker: "worker-a", task: "task-1", stalled: true },
  { type: "lease-expired", worker: "worker-a", task: "task-1" },
  { type: "assign", worker: "worker-b", task: "task-1" },
  { type: "complete", worker: "worker-b", task: "task-1" },
  { type: "assign", worker: "worker-a", task: "task-2" },
  { type: "complete", worker: "worker-a", task: "task-2" },
  { type: "done" },
];

const TERMINAL = document.querySelector("#terminal-log");
const TASK_LIST = document.querySelector("#task-list");
const WORKER_LIST = document.querySelector("#worker-list");
const ARTIFACT_LIST = document.querySelector("#artifact-list");
const EVENT_LOG = document.querySelector("#event-log");
const INPUT_FILES = document.querySelector("#input-files");
const MODE_PILL = document.querySelector("#mode-pill");

const METRICS = {
  pending: document.querySelector("#pending-count"),
  running: document.querySelector("#running-count"),
  done: document.querySelector("#done-count"),
  artifacts: document.querySelector("#artifact-count"),
};

const state = {
  mode: "standard",
  stepIndex: 0,
  timer: null,
  tasks: [],
  workers: [],
  artifacts: [],
  events: [],
  terminal: [],
};

function createBaseState() {
  return {
    tasks: [
      { id: "task-1", input: "demo-a.txt", status: "pending", progress: 0 },
      { id: "task-2", input: "demo-b.txt", status: "pending", progress: 0 },
    ],
    workers: [
      { id: "worker-a", status: "idle", task: null, detail: "Waiting for RPC assignment" },
      { id: "worker-b", status: "idle", task: null, detail: "Waiting for RPC assignment" },
    ],
    artifacts: [],
    events: [
      "coordinator listening on 127.0.0.1:9000",
      "initial task count: 2",
      "task lease: 30s",
    ],
    terminal: [
      "$ go run ./cmd/coordinator -addr 127.0.0.1:9000 -inputs demo-a.txt,demo-b.txt",
      "coordinator listening on 127.0.0.1:9000",
      "initial task count: 2",
      "task lease: 30s",
      "",
      "$ go run ./cmd/worker -addr 127.0.0.1:9000 -needle match -dir demo-out",
    ],
  };
}

function reset(mode = state.mode) {
  stopAutoPlay();
  state.mode = mode;
  state.stepIndex = 0;
  const base = createBaseState();
  state.tasks = base.tasks;
  state.workers = base.workers;
  state.artifacts = base.artifacts;
  state.events = base.events;
  state.terminal = base.terminal;
  render();
}

function currentScenario() {
  return state.mode === "lease" ? LEASE_EVENTS : STANDARD_EVENTS;
}

function taskById(id) {
  return state.tasks.find((task) => task.id === id);
}

function workerById(id) {
  return state.workers.find((worker) => worker.id === id);
}

function findFile(name) {
  return FILES.find((file) => file.name === name);
}

function grepMatches(file) {
  return file.contents
    .map((line, index) => ({ line, number: index + 1 }))
    .filter((entry) => entry.line.includes("match"))
    .map((entry) => `${entry.number}:${entry.line}`);
}

function writeArtifact(task) {
  const file = findFile(task.input);
  const matches = grepMatches(file);
  state.artifacts.push({
    name: `mr-${task.id}.jsonl`,
    contents: matches
      .map((match) => JSON.stringify({ Key: task.input, Value: match }))
      .join("\n"),
  });
}

function appendEvent(message) {
  state.events.unshift(message);
}

function appendTerminal(message) {
  state.terminal.push(message);
}

function handleAssign(step) {
  const task = taskById(step.task);
  const worker = workerById(step.worker);
  task.status = step.stalled ? "running" : "running";
  task.progress = step.stalled ? 48 : 32;
  worker.status = step.stalled ? "stalled" : "running";
  worker.task = task.id;
  worker.detail = step.stalled
    ? `Processing ${task.input} but lease is about to expire`
    : `Executing grep map on ${task.input}`;

  appendEvent(`${worker.id} requested work, coordinator assigned ${task.id}.`);
  appendTerminal(`assigned ${task.id} (map) input=${task.input}`);

  if (step.stalled) {
    appendEvent(`${worker.id} stalled before reporting completion.`);
  }
}

function handleComplete(step) {
  const task = taskById(step.task);
  const worker = workerById(step.worker);
  task.status = "done";
  task.progress = 100;
  worker.status = "idle";
  worker.task = null;
  worker.detail = `Completed ${task.id} and reported status over RPC`;
  writeArtifact(task);

  appendEvent(`${worker.id} completed ${task.id}; coordinator marked it done.`);
  appendTerminal(`completed ${task.id} (${state.artifacts.length} total)`);
}

function handleLeaseExpired(step) {
  const task = taskById(step.task);
  const worker = workerById(step.worker);
  task.status = "pending";
  task.progress = 0;
  worker.status = "idle";
  worker.task = null;
  worker.detail = `Lease expired on ${task.id}; task returned to pending`;

  appendEvent(`Lease expired for ${task.id}; coordinator reclaimed and reset the task.`);
  appendTerminal("task lease expired, coordinator reassigns the work");
}

function handleDone() {
  state.workers.forEach((worker) => {
    if (worker.status === "idle") {
      worker.detail = "Coordinator reported no remaining work";
    }
  });
  appendEvent("All tasks are terminal; subsequent workers receive done.");
  appendTerminal(`worker reached done after ${state.artifacts.length} completed task(s)`);
}

function step() {
  const scenario = currentScenario();
  if (state.stepIndex >= scenario.length) {
    stopAutoPlay();
    return;
  }

  const current = scenario[state.stepIndex];
  state.stepIndex += 1;

  if (current.type === "assign") {
    handleAssign(current);
  } else if (current.type === "complete") {
    handleComplete(current);
  } else if (current.type === "lease-expired") {
    handleLeaseExpired(current);
  } else if (current.type === "done") {
    handleDone();
  }

  render();

  if (state.stepIndex >= scenario.length) {
    stopAutoPlay();
  }
}

function startAutoPlay() {
  if (state.timer) {
    return;
  }

  state.timer = window.setInterval(() => {
    step();
  }, 1150);
}

function stopAutoPlay() {
  if (!state.timer) {
    return;
  }

  window.clearInterval(state.timer);
  state.timer = null;
}

function renderTasks() {
  TASK_LIST.innerHTML = state.tasks
    .map((task) => {
      return `
        <div class="task">
          <div class="task-top">
            <strong>${task.id}</strong>
            <span class="status ${task.status}">${task.status}</span>
          </div>
          <div class="task-meta">input: ${task.input}</div>
          <div class="task-progress"><span style="width: ${task.progress}%"></span></div>
        </div>
      `;
    })
    .join("");
}

function renderWorkers() {
  WORKER_LIST.innerHTML = state.workers
    .map((worker) => {
      return `
        <div class="worker">
          <div class="worker-top">
            <strong>${worker.id}</strong>
            <span class="status ${worker.status === "idle" ? "pending" : worker.status}">${worker.status}</span>
          </div>
          <div class="worker-detail">${worker.task ? `task: ${worker.task}` : "task: none"}</div>
          <div class="worker-detail">${worker.detail}</div>
        </div>
      `;
    })
    .join("");
}

function renderArtifacts() {
  if (state.artifacts.length === 0) {
    ARTIFACT_LIST.innerHTML = `<div class="artifact"><strong>No artifacts yet</strong><pre>Run the simulation to generate mr-task-*.jsonl files.</pre></div>`;
    return;
  }

  ARTIFACT_LIST.innerHTML = state.artifacts
    .map((artifact) => {
      return `
        <div class="artifact">
          <strong>${artifact.name}</strong>
          <pre>${artifact.contents}</pre>
        </div>
      `;
    })
    .join("");
}

function renderEvents() {
  EVENT_LOG.innerHTML = state.events.map((event) => `<li>${event}</li>`).join("");
}

function renderTerminal() {
  TERMINAL.textContent = state.terminal.join("\n");
}

function renderFiles() {
  INPUT_FILES.innerHTML = FILES.map((file) => {
    return `
      <div class="file-card">
        <strong>${file.name}</strong>
        <pre>${file.contents.join("\n")}</pre>
      </div>
    `;
  }).join("");
}

function renderMetrics() {
  const pending = state.tasks.filter((task) => task.status === "pending").length;
  const running = state.tasks.filter((task) => task.status === "running").length;
  const done = state.tasks.filter((task) => task.status === "done").length;

  METRICS.pending.textContent = String(pending);
  METRICS.running.textContent = String(running);
  METRICS.done.textContent = String(done);
  METRICS.artifacts.textContent = String(state.artifacts.length);
}

function render() {
  MODE_PILL.textContent = state.mode === "lease" ? "Lease expiry run" : "Standard run";
  renderTasks();
  renderWorkers();
  renderArtifacts();
  renderEvents();
  renderTerminal();
  renderFiles();
  renderMetrics();
}

document.querySelector("#play-demo").addEventListener("click", () => {
  startAutoPlay();
});

document.querySelector("#step-demo").addEventListener("click", () => {
  step();
});

document.querySelector("#reset-demo").addEventListener("click", () => {
  reset(state.mode);
});

document.querySelector("#lease-demo").addEventListener("click", () => {
  reset(state.mode === "lease" ? "standard" : "lease");
});

reset("standard");
