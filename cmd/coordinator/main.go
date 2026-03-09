package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"nimbus/internal/mr"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "coordinator listen address")
	inputsFlag := flag.String("inputs", "demo-a.txt,demo-b.txt", "comma-separated map task input files")
	lease := flag.Duration("lease", mr.DefaultTaskLease, "task lease duration before running work is reassigned")
	flag.Parse()

	tasks := makeTasks(*inputsFlag)
	coordinator := mr.NewCoordinatorWithLease(tasks, normalizeLease(*lease))

	listener, err := mr.StartCoordinatorRPC(coordinator, *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start coordinator rpc: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Printf("coordinator listening on %s\n", listener.Addr().String())
	fmt.Printf("initial task count: %d\n", coordinator.TaskCount())
	fmt.Printf("task lease: %s\n", normalizeLease(*lease))

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	<-signals
}

func normalizeLease(lease time.Duration) time.Duration {
	if lease <= 0 {
		return mr.DefaultTaskLease
	}

	return lease
}

func makeTasks(inputsFlag string) []mr.Task {
	parts := strings.Split(inputsFlag, ",")
	tasks := make([]mr.Task, 0, len(parts))

	for i, part := range parts {
		input := strings.TrimSpace(part)
		if input == "" {
			continue
		}

		tasks = append(tasks, mr.Task{
			ID:     fmt.Sprintf("task-%d", i+1),
			Type:   mr.TaskTypeMap,
			Input:  input,
			Status: mr.TaskStatusPending,
		})
	}

	return tasks
}
