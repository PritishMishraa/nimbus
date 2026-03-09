package mr

import (
	"errors"
	"fmt"
)

var (
	ErrTaskNotRunning       = errors.New("task is not running")
	ErrReduceNotImplemented = errors.New("reduce task execution not implemented")
)

func RunTask(c *Coordinator, taskID string, dir string, needle string, contents string) error {
	task, err := c.taskByID(taskID)
	if err != nil {
		return err
	}

	if err := ExecuteAssignedMapTask(*task, dir, needle, contents); err != nil {
		return err
	}

	if err := c.CompleteTask(task.ID); err != nil {
		return err
	}

	return nil
}

func ExecuteAssignedMapTask(task Task, dir string, needle string, contents string) error {
	if task.Status != TaskStatusRunning {
		return fmt.Errorf("%w: %s", ErrTaskNotRunning, task.ID)
	}

	switch task.Type {
	case TaskTypeMap:
		kvs, err := ExecuteMapTask(task, needle, contents)
		if err != nil {
			return err
		}

		if _, err := WriteIntermediate(task.ID, dir, kvs); err != nil {
			return err
		}

		return nil
	case TaskTypeReduce:
		return fmt.Errorf("%w: %s", ErrReduceNotImplemented, task.ID)
	default:
		return fmt.Errorf("%w: %s", ErrNotMapTask, task.ID)
	}
}
