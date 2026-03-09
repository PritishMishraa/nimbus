package mr

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRunTaskRunningMapTaskSucceeds(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "notes.txt", Status: TaskStatusPending},
	})

	task, err := c.NextTask()
	if err != nil {
		t.Fatalf("NextTask() error = %v, want nil", err)
	}

	dir := t.TempDir()
	if err := RunTask(c, task.ID, dir, "match", "alpha\nbeta match\ngamma"); err != nil {
		t.Fatalf("RunTask() error = %v, want nil", err)
	}

	path := filepath.Join(dir, IntermediateFileName(task.ID))
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("os.Stat(%q) error = %v, want nil", path, err)
	}

	if c.tasks[0].Status != TaskStatusDone {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusDone)
	}
}

func TestRunTaskRejectsPendingTask(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "notes.txt", Status: TaskStatusPending},
	})

	err := RunTask(c, "task-1", t.TempDir(), "match", "beta match")
	if !errors.Is(err, ErrTaskNotRunning) {
		t.Fatalf("RunTask() error = %v, want %v", err, ErrTaskNotRunning)
	}
}

func TestRunTaskRejectsDoneTask(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "notes.txt", Status: TaskStatusPending},
	})

	task, err := c.NextTask()
	if err != nil {
		t.Fatalf("NextTask() error = %v, want nil", err)
	}
	if err := c.CompleteTask(task.ID); err != nil {
		t.Fatalf("CompleteTask(%q) error = %v, want nil", task.ID, err)
	}

	err = RunTask(c, task.ID, t.TempDir(), "match", "beta match")
	if !errors.Is(err, ErrTaskNotRunning) {
		t.Fatalf("RunTask() error = %v, want %v", err, ErrTaskNotRunning)
	}
}

func TestRunTaskRejectsUnknownTaskID(t *testing.T) {
	t.Parallel()

	c := NewCoordinator(nil)

	err := RunTask(c, "missing", t.TempDir(), "match", "beta match")
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("RunTask() error = %v, want %v", err, ErrTaskNotFound)
	}
}

func TestRunTaskRejectsReduceTaskForNow(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeReduce, Input: "partition-0", Status: TaskStatusRunning},
	})

	err := RunTask(c, "task-1", t.TempDir(), "match", "beta match")
	if !errors.Is(err, ErrReduceNotImplemented) {
		t.Fatalf("RunTask() error = %v, want %v", err, ErrReduceNotImplemented)
	}

	if c.tasks[0].Status != TaskStatusRunning {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusRunning)
	}
}

func TestRunTaskFailedExecutionDoesNotAutoCompleteTask(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "notes.txt", Status: TaskStatusPending},
	})

	task, err := c.NextTask()
	if err != nil {
		t.Fatalf("NextTask() error = %v, want nil", err)
	}

	err = RunTask(c, task.ID, t.TempDir(), "", "beta match")
	if !errors.Is(err, ErrEmptyNeedle) {
		t.Fatalf("RunTask() error = %v, want %v", err, ErrEmptyNeedle)
	}

	if c.tasks[0].Status != TaskStatusRunning {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusRunning)
	}
}

func TestRunTaskMiniLifecycleIntegration(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "notes.txt", Status: TaskStatusPending},
	})

	task, err := c.NextTask()
	if err != nil {
		t.Fatalf("NextTask() error = %v, want nil", err)
	}

	dir := t.TempDir()
	if err := RunTask(c, task.ID, dir, "hit", "first hit\nskip\nthird hit"); err != nil {
		t.Fatalf("RunTask() error = %v, want nil", err)
	}

	if c.tasks[0].Status != TaskStatusDone {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusDone)
	}
	if !c.AllDone() {
		t.Fatal("AllDone() = false, want true")
	}
}
