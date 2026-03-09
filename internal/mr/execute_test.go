package mr

import (
	"errors"
	"reflect"
	"testing"

	"nimbus/internal/apps"
)

func TestExecuteMapTaskValid(t *testing.T) {
	t.Parallel()

	task := Task{
		ID:     "task-1",
		Type:   TaskTypeMap,
		Input:  "notes.txt",
		Status: TaskStatusPending,
	}

	contents := "alpha\nbeta match\ngamma\nmatch delta"
	needle := "match"

	got, err := ExecuteMapTask(task, needle, contents)
	if err != nil {
		t.Fatalf("ExecuteMapTask() error = %v, want nil", err)
	}

	want := []apps.KeyValue{
		{Key: "notes.txt", Value: "2:beta match"},
		{Key: "notes.txt", Value: "4:match delta"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ExecuteMapTask() = %#v, want %#v", got, want)
	}
}

func TestExecuteMapTaskRejectsReduceTask(t *testing.T) {
	t.Parallel()

	task := Task{
		ID:     "task-1",
		Type:   TaskTypeReduce,
		Input:  "notes.txt",
		Status: TaskStatusPending,
	}

	_, err := ExecuteMapTask(task, "match", "match here")
	if !errors.Is(err, ErrNotMapTask) {
		t.Fatalf("ExecuteMapTask() error = %v, want %v", err, ErrNotMapTask)
	}
}

func TestExecuteMapTaskRejectsEmptyTaskInput(t *testing.T) {
	t.Parallel()

	task := Task{
		ID:     "task-1",
		Type:   TaskTypeMap,
		Input:  "",
		Status: TaskStatusPending,
	}

	_, err := ExecuteMapTask(task, "match", "match here")
	if !errors.Is(err, ErrEmptyTaskInput) {
		t.Fatalf("ExecuteMapTask() error = %v, want %v", err, ErrEmptyTaskInput)
	}
}

func TestExecuteMapTaskRejectsEmptyNeedle(t *testing.T) {
	t.Parallel()

	task := Task{
		ID:     "task-1",
		Type:   TaskTypeMap,
		Input:  "notes.txt",
		Status: TaskStatusPending,
	}

	_, err := ExecuteMapTask(task, "", "match here")
	if !errors.Is(err, ErrEmptyNeedle) {
		t.Fatalf("ExecuteMapTask() error = %v, want %v", err, ErrEmptyNeedle)
	}
}

func TestExecuteMapTaskNoMatchesIsValid(t *testing.T) {
	t.Parallel()

	task := Task{
		ID:     "task-1",
		Type:   TaskTypeMap,
		Input:  "notes.txt",
		Status: TaskStatusPending,
	}

	got, err := ExecuteMapTask(task, "missing", "alpha\nbeta\ngamma")
	if err != nil {
		t.Fatalf("ExecuteMapTask() error = %v, want nil", err)
	}

	if len(got) != 0 {
		t.Fatalf("ExecuteMapTask() len = %d, want 0", len(got))
	}
}

func TestExecuteMapTaskDoesNotMutateTask(t *testing.T) {
	t.Parallel()

	task := Task{
		ID:     "task-1",
		Type:   TaskTypeMap,
		Input:  "notes.txt",
		Status: TaskStatusRunning,
	}
	original := task

	_, err := ExecuteMapTask(task, "match", "match here")
	if err != nil {
		t.Fatalf("ExecuteMapTask() error = %v, want nil", err)
	}

	if !reflect.DeepEqual(task, original) {
		t.Fatalf("task after ExecuteMapTask() = %#v, want %#v", task, original)
	}
}

func TestExecuteMapTaskFullMiniIntegration(t *testing.T) {
	t.Parallel()

	task := Task{
		ID:     "task-42",
		Type:   TaskTypeMap,
		Input:  "notes.txt",
		Status: TaskStatusPending,
	}

	contents := "first hit\nskip me\nthird hit\nanother skip"
	needle := "hit"

	got, err := ExecuteMapTask(task, needle, contents)
	if err != nil {
		t.Fatalf("ExecuteMapTask() error = %v, want nil", err)
	}

	want := []apps.KeyValue{
		{Key: "notes.txt", Value: "1:first hit"},
		{Key: "notes.txt", Value: "3:third hit"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ExecuteMapTask() = %#v, want %#v", got, want)
	}
}
