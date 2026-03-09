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
	ErrTaskNotResettable  = errors.New("task cannot be reset")
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
	task, err := c.taskByID(id)
	if err != nil {
		return err
	}

	if err := task.Complete(); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrTaskNotCompletable, id, err)
	}

	return nil
}

func (c *Coordinator) ResetTask(id string) error {
	task, err := c.taskByID(id)
	if err != nil {
		return err
	}

	if err := task.Reset(); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrTaskNotResettable, id, err)
	}

	return nil
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

func (c *Coordinator) taskByID(id string) (*Task, error) {
	for i := range c.tasks {
		if c.tasks[i].ID == id {
			return &c.tasks[i], nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, id)
}
