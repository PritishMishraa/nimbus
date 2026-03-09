package mr

import (
	"errors"
	"testing"
)

func TestNewCoordinatorStartsNotDone(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
		{ID: "task-2", Type: TaskTypeMap, Input: "b.txt", Status: TaskStatusPending},
	})

	if c.AllDone() {
		t.Fatal("AllDone() = true, want false")
	}
	if c.TaskCount() != 2 {
		t.Fatalf("TaskCount() = %d, want 2", c.TaskCount())
	}
	if c.PendingCount() != 2 {
		t.Fatalf("PendingCount() = %d, want 2", c.PendingCount())
	}
}

func TestNextTaskAssignsFirstPendingTask(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
		{ID: "task-2", Type: TaskTypeMap, Input: "b.txt", Status: TaskStatusPending},
	})

	task, err := c.NextTask()
	if err != nil {
		t.Fatalf("NextTask() error = %v, want nil", err)
	}
	if task == nil {
		t.Fatal("NextTask() task = nil, want non-nil")
	}
	if task.ID != "task-1" {
		t.Fatalf("NextTask() task.ID = %q, want %q", task.ID, "task-1")
	}
	if c.tasks[0].Status != TaskStatusRunning {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusRunning)
	}
}

func TestNextTaskAssignsInOrder(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
		{ID: "task-2", Type: TaskTypeMap, Input: "b.txt", Status: TaskStatusPending},
	})

	first, err := c.NextTask()
	if err != nil {
		t.Fatalf("first NextTask() error = %v, want nil", err)
	}
	second, err := c.NextTask()
	if err != nil {
		t.Fatalf("second NextTask() error = %v, want nil", err)
	}

	if first.ID != "task-1" {
		t.Fatalf("first task ID = %q, want %q", first.ID, "task-1")
	}
	if second.ID != "task-2" {
		t.Fatalf("second task ID = %q, want %q", second.ID, "task-2")
	}
	if c.tasks[0].Status != TaskStatusRunning {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusRunning)
	}
	if c.tasks[1].Status != TaskStatusRunning {
		t.Fatalf("c.tasks[1].Status = %s, want %s", c.tasks[1].Status, TaskStatusRunning)
	}
}

func TestNextTaskSkipsNonPendingTasks(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusDone},
		{ID: "task-2", Type: TaskTypeMap, Input: "b.txt", Status: TaskStatusPending},
	})

	task, err := c.NextTask()
	if err != nil {
		t.Fatalf("NextTask() error = %v, want nil", err)
	}
	if task.ID != "task-2" {
		t.Fatalf("NextTask() task.ID = %q, want %q", task.ID, "task-2")
	}
	if c.tasks[1].Status != TaskStatusRunning {
		t.Fatalf("c.tasks[1].Status = %s, want %s", c.tasks[1].Status, TaskStatusRunning)
	}
}

func TestCompleteTaskMarksRunningTaskDone(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	task, err := c.NextTask()
	if err != nil {
		t.Fatalf("NextTask() error = %v, want nil", err)
	}

	if err := c.CompleteTask(task.ID); err != nil {
		t.Fatalf("CompleteTask() error = %v, want nil", err)
	}
	if c.tasks[0].Status != TaskStatusDone {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusDone)
	}
}

func TestCompleteTaskUnknownIDReturnsError(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	err := c.CompleteTask("missing")
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("CompleteTask() error = %v, want %v", err, ErrTaskNotFound)
	}
}

func TestCompleteTaskPendingTaskReturnsError(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	err := c.CompleteTask("task-1")
	if !errors.Is(err, ErrTaskNotCompletable) {
		t.Fatalf("CompleteTask() error = %v, want %v", err, ErrTaskNotCompletable)
	}
	if !errors.Is(err, ErrTaskCannotComplete) {
		t.Fatalf("CompleteTask() error = %v, want wrapped %v", err, ErrTaskCannotComplete)
	}
}

func TestAllDoneBecomesTrueAfterAllTasksComplete(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
		{ID: "task-2", Type: TaskTypeMap, Input: "b.txt", Status: TaskStatusPending},
	})

	first, err := c.NextTask()
	if err != nil {
		t.Fatalf("first NextTask() error = %v, want nil", err)
	}
	second, err := c.NextTask()
	if err != nil {
		t.Fatalf("second NextTask() error = %v, want nil", err)
	}
	if err := c.CompleteTask(first.ID); err != nil {
		t.Fatalf("CompleteTask(%q) error = %v, want nil", first.ID, err)
	}
	if err := c.CompleteTask(second.ID); err != nil {
		t.Fatalf("CompleteTask(%q) error = %v, want nil", second.ID, err)
	}

	if !c.AllDone() {
		t.Fatal("AllDone() = false, want true")
	}
}

func TestNextTaskNoPendingCases(t *testing.T) {
	t.Parallel()

	t.Run("all running", func(t *testing.T) {
		t.Parallel()

		c := NewCoordinator([]Task{
			{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusRunning},
			{ID: "task-2", Type: TaskTypeMap, Input: "b.txt", Status: TaskStatusRunning},
		})

		task, err := c.NextTask()
		if task != nil {
			t.Fatalf("NextTask() task = %#v, want nil", task)
		}
		if !errors.Is(err, ErrNoPendingTasks) {
			t.Fatalf("NextTask() error = %v, want %v", err, ErrNoPendingTasks)
		}
	})

	t.Run("all done", func(t *testing.T) {
		t.Parallel()

		c := NewCoordinator([]Task{
			{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusDone},
			{ID: "task-2", Type: TaskTypeMap, Input: "b.txt", Status: TaskStatusDone},
		})

		task, err := c.NextTask()
		if task != nil {
			t.Fatalf("NextTask() task = %#v, want nil", task)
		}
		if !errors.Is(err, ErrAllTasksDone) {
			t.Fatalf("NextTask() error = %v, want %v", err, ErrAllTasksDone)
		}
	})
}

func TestCoordinatorMiniFlow(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
		{ID: "task-2", Type: TaskTypeMap, Input: "b.txt", Status: TaskStatusPending},
	})

	first, err := c.NextTask()
	if err != nil {
		t.Fatalf("first NextTask() error = %v, want nil", err)
	}
	if err := c.CompleteTask(first.ID); err != nil {
		t.Fatalf("CompleteTask(%q) error = %v, want nil", first.ID, err)
	}

	second, err := c.NextTask()
	if err != nil {
		t.Fatalf("second NextTask() error = %v, want nil", err)
	}
	if err := c.CompleteTask(second.ID); err != nil {
		t.Fatalf("CompleteTask(%q) error = %v, want nil", second.ID, err)
	}

	if !c.AllDone() {
		t.Fatal("AllDone() = false, want true")
	}
}
