package main

import (
	"flag"
	"fmt"
	"os"

	"nimbus/internal/mr"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "coordinator address")
	needle := flag.String("needle", "match", "grep needle")
	dir := flag.String("dir", ".", "intermediate output directory")
	flag.Parse()

	client, err := mr.DialRPC(*addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial coordinator: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	tracker := &trackingClient{RemoteCoordinator: client}
	load := func(input string) (string, error) {
		bytes, err := os.ReadFile(input)
		if err != nil {
			return "", err
		}

		return string(bytes), nil
	}

	if err := mr.RunWorkerRPC(tracker, *dir, *needle, load); err != nil {
		fmt.Fprintf(os.Stderr, "worker stopped with error after %d completed task(s): %v\n", tracker.completed, err)
		os.Exit(1)
	}

	switch {
	case tracker.sawWait:
		fmt.Printf("worker stopped on wait after %d completed task(s)\n", tracker.completed)
	case tracker.sawDone:
		fmt.Printf("worker reached done after %d completed task(s)\n", tracker.completed)
	default:
		fmt.Printf("worker stopped without terminal state after %d completed task(s)\n", tracker.completed)
	}
}

type trackingClient struct {
	mr.RemoteCoordinator
	completed int
	sawDone   bool
	sawWait   bool
}

func (c *trackingClient) RequestTask() (mr.RequestTaskResponse, error) {
	resp, err := c.RemoteCoordinator.RequestTask()
	if err != nil {
		return resp, err
	}

	c.sawDone = resp.Done
	c.sawWait = resp.Wait
	if !resp.Done && !resp.Wait {
		fmt.Printf("assigned %s (%s) input=%s\n", resp.Task.ID, resp.Task.Type, resp.Task.Input)
	}
	return resp, nil
}

func (c *trackingClient) CompleteTask(taskID string) error {
	if err := c.RemoteCoordinator.CompleteTask(taskID); err != nil {
		return err
	}

	c.completed++
	fmt.Printf("completed %s (%d total)\n", taskID, c.completed)
	return nil
}
