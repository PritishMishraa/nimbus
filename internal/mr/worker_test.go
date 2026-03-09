package mr

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestRunWorkerRunsSingleMapTaskToCompletion(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "notes.txt", Status: TaskStatusPending},
	})

	dir := t.TempDir()
	loadCalls := 0
	load := func(input string) (string, error) {
		loadCalls++
		if input != "notes.txt" {
			t.Fatalf("load() input = %q, want %q", input, "notes.txt")
		}

		return "alpha\nbeta match\ngamma", nil
	}

	if err := RunWorker(c, dir, "match", load); err != nil {
		t.Fatalf("RunWorker() error = %v, want nil", err)
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

func TestRunWorkerRunsMultipleMapTasks(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
		{ID: "task-2", Type: TaskTypeMap, Input: "b.txt", Status: TaskStatusPending},
		{ID: "task-3", Type: TaskTypeMap, Input: "c.txt", Status: TaskStatusPending},
	})

	dir := t.TempDir()
	contentsByInput := map[string]string{
		"a.txt": "alpha match\nskip",
		"b.txt": "skip\nbeta match",
		"c.txt": "gamma\nmatch delta",
	}
	loadCalls := make([]string, 0, len(contentsByInput))
	load := func(input string) (string, error) {
		loadCalls = append(loadCalls, input)

		contents, ok := contentsByInput[input]
		if !ok {
			return "", fmt.Errorf("unexpected input: %s", input)
		}

		return contents, nil
	}

	if err := RunWorker(c, dir, "match", load); err != nil {
		t.Fatalf("RunWorker() error = %v, want nil", err)
	}

	if len(loadCalls) != 3 {
		t.Fatalf("load() calls = %d, want 3", len(loadCalls))
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

func TestRunWorkerStopsCleanlyWhenAllTasksAreDone(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "notes.txt", Status: TaskStatusDone},
	})

	loadCalls := 0
	load := func(input string) (string, error) {
		loadCalls++
		return "unused", nil
	}

	if err := RunWorker(c, t.TempDir(), "match", load); err != nil {
		t.Fatalf("RunWorker() error = %v, want nil", err)
	}

	if loadCalls != 0 {
		t.Fatalf("load() calls = %d, want 0", loadCalls)
	}
}

func TestRunWorkerReturnsErrNoPendingTasksWhenTasksAreOnlyRunning(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "notes.txt", Status: TaskStatusRunning},
	})

	load := func(input string) (string, error) {
		t.Fatalf("load() called with %q, want no calls", input)
		return "", nil
	}

	err := RunWorker(c, t.TempDir(), "match", load)
	if !errors.Is(err, ErrNoPendingTasks) {
		t.Fatalf("RunWorker() error = %v, want %v", err, ErrNoPendingTasks)
	}
}

func TestRunWorkerContentProviderFailureLeavesTaskRunning(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "notes.txt", Status: TaskStatusPending},
	})

	wantErr := errors.New("load failed")
	load := func(input string) (string, error) {
		if input != "notes.txt" {
			t.Fatalf("load() input = %q, want %q", input, "notes.txt")
		}

		return "", wantErr
	}

	err := RunWorker(c, t.TempDir(), "match", load)
	if !errors.Is(err, wantErr) {
		t.Fatalf("RunWorker() error = %v, want %v", err, wantErr)
	}

	if c.tasks[0].Status != TaskStatusRunning {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusRunning)
	}
}

func TestRunWorkerRunTaskFailureLeavesTaskRunning(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "notes.txt", Status: TaskStatusPending},
	})

	load := func(input string) (string, error) {
		if input != "notes.txt" {
			t.Fatalf("load() input = %q, want %q", input, "notes.txt")
		}

		return "beta match", nil
	}

	err := RunWorker(c, t.TempDir(), "", load)
	if !errors.Is(err, ErrEmptyNeedle) {
		t.Fatalf("RunWorker() error = %v, want %v", err, ErrEmptyNeedle)
	}

	if c.tasks[0].Status != TaskStatusRunning {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusRunning)
	}
}

func TestRunWorkerMiniIntegration(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
		{ID: "task-2", Type: TaskTypeMap, Input: "b.txt", Status: TaskStatusPending},
	})

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

	if err := RunWorker(c, dir, "match", load); err != nil {
		t.Fatalf("RunWorker() error = %v, want nil", err)
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
