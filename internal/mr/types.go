package mr

import (
	"errors"
	"fmt"
)

type TaskType string

const (
	TaskTypeMap    TaskType = "map"
	TaskTypeReduce TaskType = "reduce"
	TaskTypeIdle   TaskType = "idle"
)

type TaskStatus string

const (
	TaskStatusPending TaskStatus = "pending"
	TaskStatusRunning TaskStatus = "running"
	TaskStatusDone    TaskStatus = "done"
)

type Task struct {
	ID     string
	Type   TaskType
	Input  string
	Status TaskStatus
}

var (
	ErrTaskCannotStart    = errors.New("invalid task start transition")
	ErrTaskCannotComplete = errors.New("invalid task complete transition")
	ErrTaskCannotReset    = errors.New("invalid task reset transition")
)

func (t Task) IsTerminal() bool {
	return t.Status == TaskStatusDone
}

func (t *Task) Start() error {
	if t.Status != TaskStatusPending {
		return fmt.Errorf("%w: cannot start task from status %s", ErrTaskCannotStart, t.Status)
	}

	t.Status = TaskStatusRunning
	return nil
}

func (t *Task) Complete() error {
	if t.Status != TaskStatusRunning {
		return fmt.Errorf("%w: cannot complete task from status %s", ErrTaskCannotComplete, t.Status)
	}

	t.Status = TaskStatusDone
	return nil
}

func (t *Task) Reset() error {
	if t.Status != TaskStatusRunning {
		return fmt.Errorf("%w: cannot reset task from status %s", ErrTaskCannotReset, t.Status)
	}

	t.Status = TaskStatusPending
	return nil
}
