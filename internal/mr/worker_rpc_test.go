package mr

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestRunWorkerRPCDrainsSingleTask(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "notes.txt", Status: TaskStatusPending},
	})

	_, client := startTestRPCClient(t, c)

	dir := t.TempDir()
	loadCalls := 0
	load := func(input string) (string, error) {
		loadCalls++
		if input != "notes.txt" {
			t.Fatalf("load() input = %q, want %q", input, "notes.txt")
		}

		return "alpha\nbeta match\ngamma", nil
	}

	if err := RunWorkerRPC(client, dir, "match", load); err != nil {
		t.Fatalf("RunWorkerRPC() error = %v, want nil", err)
	}

	if loadCalls != 1 {
		t.Fatalf("load() calls = %d, want 1", loadCalls)
	}

	path := filepath.Join(dir, IntermediateFileName("task-1"))
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("os.Stat(%q) error = %v, want nil", path, err)
	}

	if !c.AllDone() {
		t.Fatal("AllDone() = false, want true")
	}
}

func TestRunWorkerRPCDrainsMultipleTasks(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
		{ID: "task-2", Type: TaskTypeMap, Input: "b.txt", Status: TaskStatusPending},
		{ID: "task-3", Type: TaskTypeMap, Input: "c.txt", Status: TaskStatusPending},
	})

	_, client := startTestRPCClient(t, c)

	dir := t.TempDir()
	contentsByInput := map[string]string{
		"a.txt": "alpha match\nskip",
		"b.txt": "skip\nbeta match",
		"c.txt": "gamma\nmatch delta",
	}
	load := func(input string) (string, error) {
		contents, ok := contentsByInput[input]
		if !ok {
			return "", fmt.Errorf("unexpected input: %s", input)
		}

		return contents, nil
	}

	if err := RunWorkerRPC(client, dir, "match", load); err != nil {
		t.Fatalf("RunWorkerRPC() error = %v, want nil", err)
	}

	for _, id := range []string{"task-1", "task-2", "task-3"} {
		path := filepath.Join(dir, IntermediateFileName(id))
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("os.Stat(%q) error = %v, want nil", path, err)
		}
	}

	if !c.AllDone() {
		t.Fatal("AllDone() = false, want true")
	}
}

func TestRunWorkerRPCStopsCleanlyOnDone(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "notes.txt", Status: TaskStatusDone},
	})

	_, client := startTestRPCClient(t, c)

	loadCalls := 0
	load := func(input string) (string, error) {
		loadCalls++
		return "unused", nil
	}

	if err := RunWorkerRPC(client, t.TempDir(), "match", load); err != nil {
		t.Fatalf("RunWorkerRPC() error = %v, want nil", err)
	}

	if loadCalls != 0 {
		t.Fatalf("load() calls = %d, want 0", loadCalls)
	}
}

func TestRunWorkerRPCStopsCleanlyOnWait(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "notes.txt", Status: TaskStatusRunning},
	})

	_, client := startTestRPCClient(t, c)

	dir := t.TempDir()
	load := func(input string) (string, error) {
		t.Fatalf("load() called with %q, want no calls", input)
		return "", nil
	}

	if err := RunWorkerRPC(client, dir, "match", load); err != nil {
		t.Fatalf("RunWorkerRPC() error = %v, want nil", err)
	}

	path := filepath.Join(dir, IntermediateFileName("task-1"))
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("os.Stat(%q) error = %v, want %v", path, err, os.ErrNotExist)
	}
}

func TestRunWorkerRPCProviderFailureLeavesRemoteTaskRunning(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "notes.txt", Status: TaskStatusPending},
	})

	_, client := startTestRPCClient(t, c)

	wantErr := errors.New("load failed")
	load := func(input string) (string, error) {
		if input != "notes.txt" {
			t.Fatalf("load() input = %q, want %q", input, "notes.txt")
		}

		return "", wantErr
	}

	err := RunWorkerRPC(client, t.TempDir(), "match", load)
	if !errors.Is(err, wantErr) {
		t.Fatalf("RunWorkerRPC() error = %v, want %v", err, wantErr)
	}

	if c.tasks[0].Status != TaskStatusRunning {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusRunning)
	}
}

func TestRunWorkerRPCExecutionFailureLeavesRemoteTaskRunning(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "notes.txt", Status: TaskStatusPending},
	})

	_, client := startTestRPCClient(t, c)

	load := func(input string) (string, error) {
		if input != "notes.txt" {
			t.Fatalf("load() input = %q, want %q", input, "notes.txt")
		}

		return "beta match", nil
	}

	err := RunWorkerRPC(client, t.TempDir(), "", load)
	if !errors.Is(err, ErrEmptyNeedle) {
		t.Fatalf("RunWorkerRPC() error = %v, want %v", err, ErrEmptyNeedle)
	}

	if c.tasks[0].Status != TaskStatusRunning {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusRunning)
	}
}

func TestRunWorkerRPCSurfacesRemoteCompleteFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("complete failed")
	client := &fakeRemoteCoordinator{
		requests: []RequestTaskResponse{
			{
				Task: Task{ID: "task-1", Type: TaskTypeMap, Input: "notes.txt", Status: TaskStatusRunning},
			},
		},
		completeErr: wantErr,
	}

	load := func(input string) (string, error) {
		if input != "notes.txt" {
			t.Fatalf("load() input = %q, want %q", input, "notes.txt")
		}

		return "alpha\nbeta match", nil
	}

	err := RunWorkerRPC(client, t.TempDir(), "match", load)
	if !errors.Is(err, wantErr) {
		t.Fatalf("RunWorkerRPC() error = %v, want %v", err, wantErr)
	}

	if client.completeCalls != 1 {
		t.Fatalf("CompleteTask() calls = %d, want 1", client.completeCalls)
	}
}

func TestRunWorkerRPCMiniEndToEndFlow(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
		{ID: "task-2", Type: TaskTypeMap, Input: "b.txt", Status: TaskStatusPending},
	})

	_, client := startTestRPCClient(t, c)

	dir := t.TempDir()
	load := func(input string) (string, error) {
		switch input {
		case "a.txt":
			return "match one\nskip", nil
		case "b.txt":
			return "skip\nmatch two", nil
		default:
			return "", fmt.Errorf("unexpected input: %s", input)
		}
	}

	if err := RunWorkerRPC(client, dir, "match", load); err != nil {
		t.Fatalf("RunWorkerRPC() error = %v, want nil", err)
	}

	for _, id := range []string{"task-1", "task-2"} {
		path := filepath.Join(dir, IntermediateFileName(id))
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("os.Stat(%q) error = %v, want nil", path, err)
		}
	}

	if !c.AllDone() {
		t.Fatal("AllDone() = false, want true")
	}
}

type fakeRemoteCoordinator struct {
	requests      []RequestTaskResponse
	requestErr    error
	completeErr   error
	requestCalls  int
	completeCalls int
	resetCalls    int
}

func (f *fakeRemoteCoordinator) RequestTask() (RequestTaskResponse, error) {
	f.requestCalls++
	if f.requestErr != nil {
		return RequestTaskResponse{}, f.requestErr
	}
	if len(f.requests) == 0 {
		return RequestTaskResponse{Done: true}, nil
	}

	resp := f.requests[0]
	f.requests = f.requests[1:]
	return resp, nil
}

func (f *fakeRemoteCoordinator) CompleteTask(taskID string) error {
	f.completeCalls++
	return f.completeErr
}

func (f *fakeRemoteCoordinator) ResetTask(taskID string) error {
	f.resetCalls++
	return nil
}
