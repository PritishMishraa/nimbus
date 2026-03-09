package mr

import (
	"errors"

	"nimbus/internal/apps"
)

var (
	ErrNotMapTask     = errors.New("task is not a map task")
	ErrEmptyTaskInput = errors.New("task input is empty")
	ErrEmptyNeedle    = errors.New("needle is empty")
)

func ExecuteMapTask(task Task, needle string, contents string) ([]apps.KeyValue, error) {
	if task.Type != TaskTypeMap {
		return nil, ErrNotMapTask
	}

	if task.Input == "" {
		return nil, ErrEmptyTaskInput
	}

	if needle == "" {
		return nil, ErrEmptyNeedle
	}

	return apps.MapGrep(task.Input, contents, needle), nil
}
