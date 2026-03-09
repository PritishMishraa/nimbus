package mr

import (
	"errors"
	"fmt"
)

type Coordinator struct {
	tasks []Task
}

var (
	ErrNoPendingTasks     = errors.New("no pending tasks")
	ErrAllTasksDone       = errors.New("all tasks done")
	ErrTaskNotFound       = errors.New("task not found")
	ErrTaskNotCompletable = errors.New("task cannot be completed")
)

func NewCoordinator(tasks []Task) *Coordinator {
	ownedTasks := make([]Task, len(tasks))
	copy(ownedTasks, tasks)

	return &Coordinator{tasks: ownedTasks}
}

func (c *Coordinator) NextTask() (*Task, error) {
	for i := range c.tasks {
		if c.tasks[i].Status != TaskStatusPending {
			continue
		}

		if err := c.tasks[i].Start(); err != nil {
			return nil, err
		}

		return &c.tasks[i], nil
	}

	if c.AllDone() {
		return nil, ErrAllTasksDone
	}

	return nil, ErrNoPendingTasks
}

func (c *Coordinator) CompleteTask(id string) error {
	for i := range c.tasks {
		if c.tasks[i].ID != id {
			continue
		}

		if err := c.tasks[i].Complete(); err != nil {
			return fmt.Errorf("%w: %s: %w", ErrTaskNotCompletable, id, err)
		}

		return nil
	}

	return fmt.Errorf("%w: %s", ErrTaskNotFound, id)
}

func (c *Coordinator) AllDone() bool {
	for i := range c.tasks {
		if !c.tasks[i].IsTerminal() {
			return false
		}
	}

	return true
}

func (c *Coordinator) TaskCount() int {
	return len(c.tasks)
}

func (c *Coordinator) PendingCount() int {
	count := 0
	for i := range c.tasks {
		if c.tasks[i].Status == TaskStatusPending {
			count++
		}
	}

	return count
}
