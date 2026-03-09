package mr

import (
	"net/rpc"
)

const (
	coordinatorRequestTaskRPC  = "Coordinator.RequestTask"
	coordinatorCompleteTaskRPC = "Coordinator.CompleteTaskRPC"
	coordinatorResetTaskRPC    = "Coordinator.ResetTaskRPC"
)

type RPCClient struct {
	client *rpc.Client
}

func DialRPC(addr string) (*RPCClient, error) {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &RPCClient{client: client}, nil
}

func (c *RPCClient) RequestTask() (RequestTaskResponse, error) {
	var resp RequestTaskResponse
	err := c.client.Call(coordinatorRequestTaskRPC, RequestTaskRequest{}, &resp)
	return resp, err
}

func (c *RPCClient) CompleteTask(taskID string) error {
	return c.client.Call(coordinatorCompleteTaskRPC, CompleteTaskRequest{TaskID: taskID}, &CompleteTaskResponse{})
}

func (c *RPCClient) ResetTask(taskID string) error {
	return c.client.Call(coordinatorResetTaskRPC, ResetTaskRequest{TaskID: taskID}, &ResetTaskResponse{})
}

func (c *RPCClient) Close() error {
	return c.client.Close()
}
