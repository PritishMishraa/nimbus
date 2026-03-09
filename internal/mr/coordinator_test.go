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

func TestResetTaskResetsRunningTask(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	task, err := c.NextTask()
	if err != nil {
		t.Fatalf("NextTask() error = %v, want nil", err)
	}

	if err := c.ResetTask(task.ID); err != nil {
		t.Fatalf("ResetTask(%q) error = %v, want nil", task.ID, err)
	}
	if c.tasks[0].Status != TaskStatusPending {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusPending)
	}
}

func TestResetTaskUnknownIDReturnsError(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	err := c.ResetTask("missing")
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("ResetTask() error = %v, want %v", err, ErrTaskNotFound)
	}
}

func TestResetTaskPendingTaskReturnsError(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	err := c.ResetTask("task-1")
	if !errors.Is(err, ErrTaskNotResettable) {
		t.Fatalf("ResetTask() error = %v, want %v", err, ErrTaskNotResettable)
	}
	if !errors.Is(err, ErrTaskCannotReset) {
		t.Fatalf("ResetTask() error = %v, want wrapped %v", err, ErrTaskCannotReset)
	}
}

func TestResetTaskDoneTaskReturnsError(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	task, err := c.NextTask()
	if err != nil {
		t.Fatalf("NextTask() error = %v, want nil", err)
	}
	if err := c.CompleteTask(task.ID); err != nil {
		t.Fatalf("CompleteTask(%q) error = %v, want nil", task.ID, err)
	}

	err = c.ResetTask(task.ID)
	if !errors.Is(err, ErrTaskNotResettable) {
		t.Fatalf("ResetTask() error = %v, want %v", err, ErrTaskNotResettable)
	}
	if !errors.Is(err, ErrTaskCannotReset) {
		t.Fatalf("ResetTask() error = %v, want wrapped %v", err, ErrTaskCannotReset)
	}
}

func TestResetTaskMakesTaskAssignableAgain(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	first, err := c.NextTask()
	if err != nil {
		t.Fatalf("first NextTask() error = %v, want nil", err)
	}
	if err := c.ResetTask(first.ID); err != nil {
		t.Fatalf("ResetTask(%q) error = %v, want nil", first.ID, err)
	}

	second, err := c.NextTask()
	if err != nil {
		t.Fatalf("second NextTask() error = %v, want nil", err)
	}
	if second.ID != first.ID {
		t.Fatalf("second task ID = %q, want %q", second.ID, first.ID)
	}
	if c.tasks[0].Status != TaskStatusRunning {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusRunning)
	}
}

func TestResetTaskDoesNotAffectOtherTasks(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
		{ID: "task-2", Type: TaskTypeMap, Input: "b.txt", Status: TaskStatusPending},
	})

	task, err := c.NextTask()
	if err != nil {
		t.Fatalf("NextTask() error = %v, want nil", err)
	}
	if err := c.ResetTask(task.ID); err != nil {
		t.Fatalf("ResetTask(%q) error = %v, want nil", task.ID, err)
	}

	if c.tasks[0].Status != TaskStatusPending {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusPending)
	}
	if c.tasks[1].Status != TaskStatusPending {
		t.Fatalf("c.tasks[1].Status = %s, want %s", c.tasks[1].Status, TaskStatusPending)
	}
	if c.tasks[1].ID != "task-2" {
		t.Fatalf("c.tasks[1].ID = %q, want %q", c.tasks[1].ID, "task-2")
	}
	if c.tasks[1].Input != "b.txt" {
		t.Fatalf("c.tasks[1].Input = %q, want %q", c.tasks[1].Input, "b.txt")
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

func TestCoordinatorMiniFaultToleranceFlow(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	first, err := c.NextTask()
	if err != nil {
		t.Fatalf("first NextTask() error = %v, want nil", err)
	}
	if first.Status != TaskStatusRunning {
		t.Fatalf("first task status = %s, want %s", first.Status, TaskStatusRunning)
	}

	if err := c.ResetTask(first.ID); err != nil {
		t.Fatalf("ResetTask(%q) error = %v, want nil", first.ID, err)
	}
	if c.tasks[0].Status != TaskStatusPending {
		t.Fatalf("c.tasks[0].Status after reset = %s, want %s", c.tasks[0].Status, TaskStatusPending)
	}

	second, err := c.NextTask()
	if err != nil {
		t.Fatalf("second NextTask() error = %v, want nil", err)
	}
	if second.ID != first.ID {
		t.Fatalf("second task ID = %q, want %q", second.ID, first.ID)
	}
	if second.Status != TaskStatusRunning {
		t.Fatalf("second task status = %s, want %s", second.Status, TaskStatusRunning)
	}

	if err := c.CompleteTask(second.ID); err != nil {
		t.Fatalf("CompleteTask(%q) error = %v, want nil", second.ID, err)
	}
	if c.tasks[0].Status != TaskStatusDone {
		t.Fatalf("c.tasks[0].Status after completion = %s, want %s", c.tasks[0].Status, TaskStatusDone)
	}
}
