package mr

import (
	"net"
	"testing"
)

func TestStartCoordinatorRPCAndDialClient(t *testing.T) {
	t.Parallel()

	c := NewCoordinator(nil)

	listener, client := startTestRPCClient(t, c)

	if listener.Addr().String() == "" {
		t.Fatal("listener.Addr().String() = empty, want bound tcp address")
	}
	if client == nil {
		t.Fatal("DialRPC() returned nil client")
	}
}

func TestRequestTaskOverRPC(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	_, client := startTestRPCClient(t, c)

	resp, err := client.RequestTask()
	if err != nil {
		t.Fatalf("RequestTask() error = %v, want nil", err)
	}

	if resp.Done {
		t.Fatal("RequestTask() resp.Done = true, want false")
	}
	if resp.Wait {
		t.Fatal("RequestTask() resp.Wait = true, want false")
	}
	if resp.Task.ID != "task-1" {
		t.Fatalf("RequestTask() resp.Task.ID = %q, want %q", resp.Task.ID, "task-1")
	}
	if resp.Task.Type != TaskTypeMap {
		t.Fatalf("RequestTask() resp.Task.Type = %q, want %q", resp.Task.Type, TaskTypeMap)
	}
	if resp.Task.Input != "a.txt" {
		t.Fatalf("RequestTask() resp.Task.Input = %q, want %q", resp.Task.Input, "a.txt")
	}
	if c.tasks[0].Status != TaskStatusRunning {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusRunning)
	}
}

func TestCompleteTaskOverRPC(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	_, client := startTestRPCClient(t, c)

	resp, err := client.RequestTask()
	if err != nil {
		t.Fatalf("RequestTask() error = %v, want nil", err)
	}

	if err := client.CompleteTask(resp.Task.ID); err != nil {
		t.Fatalf("CompleteTask(%q) error = %v, want nil", resp.Task.ID, err)
	}
	if c.tasks[0].Status != TaskStatusDone {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusDone)
	}
}

func TestResetTaskOverRPC(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	_, client := startTestRPCClient(t, c)

	resp, err := client.RequestTask()
	if err != nil {
		t.Fatalf("RequestTask() error = %v, want nil", err)
	}

	if err := client.ResetTask(resp.Task.ID); err != nil {
		t.Fatalf("ResetTask(%q) error = %v, want nil", resp.Task.ID, err)
	}
	if c.tasks[0].Status != TaskStatusPending {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusPending)
	}
}

func TestWaitStateOverRPC(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusRunning},
	})

	_, client := startTestRPCClient(t, c)

	resp, err := client.RequestTask()
	if err != nil {
		t.Fatalf("RequestTask() error = %v, want nil", err)
	}

	if resp.Done {
		t.Fatal("RequestTask() resp.Done = true, want false")
	}
	if !resp.Wait {
		t.Fatal("RequestTask() resp.Wait = false, want true")
	}
}

func TestDoneStateOverRPC(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusDone},
	})

	_, client := startTestRPCClient(t, c)

	resp, err := client.RequestTask()
	if err != nil {
		t.Fatalf("RequestTask() error = %v, want nil", err)
	}

	if !resp.Done {
		t.Fatal("RequestTask() resp.Done = false, want true")
	}
	if resp.Wait {
		t.Fatal("RequestTask() resp.Wait = true, want false")
	}
}

func TestRequestTaskCopySafetyOverRPC(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	_, client := startTestRPCClient(t, c)

	resp, err := client.RequestTask()
	if err != nil {
		t.Fatalf("RequestTask() error = %v, want nil", err)
	}

	resp.Task.ID = "worker-local"
	resp.Task.Status = TaskStatusDone

	if c.tasks[0].ID != "task-1" {
		t.Fatalf("c.tasks[0].ID = %q, want %q", c.tasks[0].ID, "task-1")
	}
	if c.tasks[0].Status != TaskStatusRunning {
		t.Fatalf("c.tasks[0].Status = %s, want %s", c.tasks[0].Status, TaskStatusRunning)
	}
}

func TestRPCMiniNetworkIntegration(t *testing.T) {
	t.Parallel()

	c := NewCoordinator([]Task{
		{ID: "task-1", Type: TaskTypeMap, Input: "a.txt", Status: TaskStatusPending},
	})

	_, client := startTestRPCClient(t, c)

	firstResp, err := client.RequestTask()
	if err != nil {
		t.Fatalf("RequestTask() error = %v, want nil", err)
	}
	if err := client.CompleteTask(firstResp.Task.ID); err != nil {
		t.Fatalf("CompleteTask(%q) error = %v, want nil", firstResp.Task.ID, err)
	}

	secondResp, err := client.RequestTask()
	if err != nil {
		t.Fatalf("second RequestTask() error = %v, want nil", err)
	}
	if !secondResp.Done {
		t.Fatal("second RequestTask() resp.Done = false, want true")
	}
	if secondResp.Wait {
		t.Fatal("second RequestTask() resp.Wait = true, want false")
	}
}

func startTestRPCClient(t *testing.T, c *Coordinator) (net.Listener, *RPCClient) {
	t.Helper()

	listener, err := StartCoordinatorRPC(c, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("StartCoordinatorRPC() error = %v, want nil", err)
	}
	t.Cleanup(func() {
		if err := listener.Close(); err != nil {
			t.Fatalf("listener.Close() error = %v, want nil", err)
		}
	})

	client, err := DialRPC(listener.Addr().String())
	if err != nil {
		t.Fatalf("DialRPC(%q) error = %v, want nil", listener.Addr().String(), err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Fatalf("client.Close() error = %v, want nil", err)
		}
	})

	return listener, client
}
