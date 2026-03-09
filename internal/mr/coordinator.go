package mr

import (
	"errors"
	"fmt"
	"time"
)

type Coordinator struct {
	tasks         []Task
	leaseDuration time.Duration
	now           func() time.Time
}

var (
	ErrNoPendingTasks     = errors.New("no pending tasks")
	ErrAllTasksDone       = errors.New("all tasks done")
	ErrTaskNotFound       = errors.New("task not found")
	ErrTaskNotCompletable = errors.New("task cannot be completed")
	ErrTaskNotResettable  = errors.New("task cannot be reset")
)

const DefaultTaskLease = 30 * time.Second

func NewCoordinator(tasks []Task) *Coordinator {
	return NewCoordinatorWithLease(tasks, DefaultTaskLease)
}

func NewCoordinatorWithLease(tasks []Task, leaseDuration time.Duration) *Coordinator {
	ownedTasks := make([]Task, len(tasks))
	copy(ownedTasks, tasks)

	return &Coordinator{
		tasks:         ownedTasks,
		leaseDuration: leaseDuration,
		now:           time.Now,
	}
}

func (c *Coordinator) NextTask() (*Task, error) {
	c.reclaimExpiredTasks()

	for i := range c.tasks {
		if c.tasks[i].Status != TaskStatusPending {
			continue
		}

		if err := c.tasks[i].StartLease(c.now(), c.leaseDuration); err != nil {
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

func (c *Coordinator) reclaimExpiredTasks() {
	now := c.now()
	for i := range c.tasks {
		if !c.tasks[i].LeaseExpired(now) {
			continue
		}

		_ = c.tasks[i].Reset()
	}
}
