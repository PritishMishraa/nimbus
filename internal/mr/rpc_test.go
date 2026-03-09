package mr

import (
	"errors"
	"testing"
)

func TestRequestTaskReturnsTaskWhenPendingWorkExists(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	var resp RequestTaskResponse
	if err := c.RequestTask(RequestTaskRequest{}, &resp); err != nil {
		t.Fatalf("RequestTask() error = %v, want nil", err)
	}

	if resp.Done {
		t.Fatal("RequestTask() resp.Done = true, want false")
	}
	if resp.Wait {
		t.Fatal("RequestTask() resp.Wait = true, want false")
	}
	if resp.Task.ID != "task-1" {
		t.Fatalf("RequestTask() resp.Task.ID = %q, want %q", resp.Task.ID, "task-1")
	}
	if resp.Task.Type != TaskTypeMap {
		t.Fatalf("RequestTask() resp.Task.Type = %q, want %q", resp.Task.Type, TaskTypeMap)
	}
	if resp.Task.Input != "a.txt" {
		t.Fatalf("RequestTask() resp.Task.Input = %q, want %q", resp.Task.Input, "a.txt")
	}
	if c.tasks[0].Status != TaskStatusRunning {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusRunning)
	}
}

func TestRequestTaskReturnsDoneWhenAllTasksAreDone(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusDone},
	})

	var resp RequestTaskResponse
	if err := c.RequestTask(RequestTaskRequest{}, &resp); err != nil {
		t.Fatalf("RequestTask() error = %v, want nil", err)
	}

	if !resp.Done {
		t.Fatal("RequestTask() resp.Done = false, want true")
	}
	if resp.Wait {
		t.Fatal("RequestTask() resp.Wait = true, want false")
	}
}

func TestRequestTaskReturnsWaitWhenNoPendingTasksRemain(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusRunning},
	})

	var resp RequestTaskResponse
	if err := c.RequestTask(RequestTaskRequest{}, &resp); err != nil {
		t.Fatalf("RequestTask() error = %v, want nil", err)
	}

	if resp.Done {
		t.Fatal("RequestTask() resp.Done = true, want false")
	}
	if !resp.Wait {
		t.Fatal("RequestTask() resp.Wait = false, want true")
	}
}

func TestRequestTaskReturnsTaskCopy(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	var resp RequestTaskResponse
	if err := c.RequestTask(RequestTaskRequest{}, &resp); err != nil {
		t.Fatalf("RequestTask() error = %v, want nil", err)
	}

	resp.Task.ID = "mutated"
	resp.Task.Status = TaskStatusDone

	if c.tasks[0].ID != "task-1" {
		t.Fatalf("c.tasks[0].ID = %q, want %q", c.tasks[0].ID, "task-1")
	}
	if c.tasks[0].Status != TaskStatusRunning {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusRunning)
	}
}

func TestCompleteTaskRPCSucceedsForRunningTask(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	var requestResp RequestTaskResponse
	if err := c.RequestTask(RequestTaskRequest{}, &requestResp); err != nil {
		t.Fatalf("RequestTask() error = %v, want nil", err)
	}

	if err := c.CompleteTaskRPC(CompleteTaskRequest{TaskID: requestResp.Task.ID}, &CompleteTaskResponse{}); err != nil {
		t.Fatalf("CompleteTaskRPC() error = %v, want nil", err)
	}
	if c.tasks[0].Status != TaskStatusDone {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusDone)
	}
}

func TestCompleteTaskRPCFailsOnUnknownTask(t *testing.T) {
	t.Parallel()

	c := NewCoordinator(nil)

	err := c.CompleteTaskRPC(CompleteTaskRequest{TaskID: "missing"}, &CompleteTaskResponse{})
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("CompleteTaskRPC() error = %v, want %v", err, ErrTaskNotFound)
	}
}

func TestResetTaskRPCSucceedsForRunningTask(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	var requestResp RequestTaskResponse
	if err := c.RequestTask(RequestTaskRequest{}, &requestResp); err != nil {
		t.Fatalf("RequestTask() error = %v, want nil", err)
	}

	if err := c.ResetTaskRPC(ResetTaskRequest{TaskID: requestResp.Task.ID}, &ResetTaskResponse{}); err != nil {
		t.Fatalf("ResetTaskRPC() error = %v, want nil", err)
	}
	if c.tasks[0].Status != TaskStatusPending {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusPending)
	}
}

func TestResetTaskRPCFailsForDoneTask(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	var requestResp RequestTaskResponse
	if err := c.RequestTask(RequestTaskRequest{}, &requestResp); err != nil {
		t.Fatalf("RequestTask() error = %v, want nil", err)
	}
	if err := c.CompleteTaskRPC(CompleteTaskRequest{TaskID: requestResp.Task.ID}, &CompleteTaskResponse{}); err != nil {
		t.Fatalf("CompleteTaskRPC() error = %v, want nil", err)
	}

	err := c.ResetTaskRPC(ResetTaskRequest{TaskID: requestResp.Task.ID}, &ResetTaskResponse{})
	if !errors.Is(err, ErrTaskNotResettable) {
		t.Fatalf("ResetTaskRPC() error = %v, want %v", err, ErrTaskNotResettable)
	}
	if !errors.Is(err, ErrTaskCannotReset) {
		t.Fatalf("ResetTaskRPC() error = %v, want wrapped %v", err, ErrTaskCannotReset)
	}
}

func TestRPCMiniFlow(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	var requestResp RequestTaskResponse
	if err := c.RequestTask(RequestTaskRequest{}, &requestResp); err != nil {
		t.Fatalf("RequestTask() error = %v, want nil", err)
	}

	requestResp.Task.ID = "worker-local"
	requestResp.Task.Status = TaskStatusDone

	if err := c.CompleteTaskRPC(CompleteTaskRequest{TaskID: "task-1"}, &CompleteTaskResponse{}); err != nil {
		t.Fatalf("CompleteTaskRPC() error = %v, want nil", err)
	}
	if !c.AllDone() {
		t.Fatal("AllDone() = false, want true")
	}
	if c.tasks[0].ID != "task-1" {
		t.Fatalf("c.tasks[0].ID = %q, want %q", c.tasks[0].ID, "task-1")
	}
}
