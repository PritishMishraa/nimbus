package mr

import "errors"

type RequestTaskRequest struct{}

type RequestTaskResponse struct {
	Task Task
	Done bool
	Wait bool
}

type CompleteTaskRequest struct {
	TaskID string
}

type CompleteTaskResponse struct{}

type ResetTaskRequest struct {
	TaskID string
}

type ResetTaskResponse struct{}

func (c *Coordinator) RequestTask(_ RequestTaskRequest, resp *RequestTaskResponse) error {
	task, err := c.NextTask()
	if err != nil {
		if errors.Is(err, ErrAllTasksDone) {
			*resp = RequestTaskResponse{Done: true}
			return nil
		}
		if errors.Is(err, ErrNoPendingTasks) {
			*resp = RequestTaskResponse{Wait: true}
			return nil
		}

		return err
	}

	*resp = RequestTaskResponse{
		Task: *task,
	}
	return nil
}

func (c *Coordinator) CompleteTaskRPC(req CompleteTaskRequest, _ *CompleteTaskResponse) error {
	return c.CompleteTask(req.TaskID)
}

func (c *Coordinator) ResetTaskRPC(req ResetTaskRequest, _ *ResetTaskResponse) error {
	return c.ResetTask(req.TaskID)
}
