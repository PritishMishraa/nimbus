# Project nimbus

Nimbus is a small MapReduce-style system in Go with a TCP RPC coordinator and a separate worker process. The current implementation is focused on the map phase of a grep-style workload: the coordinator hands out map tasks over RPC, the worker loads input files locally, writes intermediate JSONL artifacts, and reports completion back to the coordinator.

## Current scope

- TCP RPC coordinator server
- RPC-only worker loop
- Remote task assignment with task copies
- Grep-style map execution
- Intermediate artifact writing (`mr-<task-id>.jsonl`)
- Local multi-process demo with separate coordinator and worker terminals

Not implemented yet:

- reduce execution
- automatic lease/timeout reassignment
- durable coordinator state

## Build and test

```bash
make build
make test
```

## Two-terminal manual demo

From the repo root, create two small input files:

```bash
cat > demo-a.txt <<'EOF'
alpha
match here
omega
EOF

cat > demo-b.txt <<'EOF'
skip
another match
EOF
```

Start the coordinator in terminal 1:

```bash
go run ./cmd/coordinator -addr 127.0.0.1:9000 -inputs demo-a.txt,demo-b.txt
```

Expected startup output:

```text
coordinator listening on 127.0.0.1:9000
initial task count: 2
```

Run the worker in terminal 2:

```bash
mkdir -p demo-out
go run ./cmd/worker -addr 127.0.0.1:9000 -needle match -dir demo-out
```

Expected worker flow:

```text
assigned task-1 (map) input=demo-a.txt
completed task-1 (1 total)
assigned task-2 (map) input=demo-b.txt
completed task-2 (2 total)
worker reached done after 2 completed task(s)
```

Run the worker a second time against the same coordinator:

```bash
go run ./cmd/worker -addr 127.0.0.1:9000 -needle match -dir demo-out
```

Expected second-run output:

```text
worker reached done after 0 completed task(s)
```

Inspect the intermediate artifacts:

```bash
ls demo-out
cat demo-out/mr-task-1.jsonl
cat demo-out/mr-task-2.jsonl
```

The JSONL files contain grep matches as key/value records keyed by input filename.

## Scripted demo

For a repeatable local demo that creates inputs, runs the coordinator, runs the worker twice, and prints the output artifact contents:

```bash
make demo
```

The script stores its temporary files in a fresh directory under `/tmp` and prints that path before exiting.

## Architecture

- [`cmd/coordinator`](/Users/pritishmishra/Documents/Projects/nimbus/cmd/coordinator/main.go) starts the TCP RPC server and seeds map tasks from `-inputs`.
- [`cmd/worker`](/Users/pritishmishra/Documents/Projects/nimbus/cmd/worker/main.go) dials the coordinator, pulls tasks over RPC, executes grep map work, and reports completion remotely.
- [`internal/mr/coordinator.go`](/Users/pritishmishra/Documents/Projects/nimbus/internal/mr/coordinator.go) owns task lifecycle transitions (`pending -> running -> done`, plus manual reset from `running -> pending`).
- [`internal/mr/rpc.go`](/Users/pritishmishra/Documents/Projects/nimbus/internal/mr/rpc.go) exposes the coordinator RPC methods for `RequestTask`, `CompleteTaskRPC`, and `ResetTaskRPC`.
- [`internal/mr/worker_rpc.go`](/Users/pritishmishra/Documents/Projects/nimbus/internal/mr/worker_rpc.go) is the RPC-backed worker loop.
- [`internal/apps/grep.go`](/Users/pritishmishra/Documents/Projects/nimbus/internal/apps/grep.go) implements the grep-style map logic.

## Limitations

- A worker stops immediately on `wait`; it does not poll for more work.
- If a worker fails after assignment, the task remains `running` until manually reset.
- Inputs are expected to be local files visible to the worker process.
