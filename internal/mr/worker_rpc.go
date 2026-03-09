package mr

type RemoteCoordinator interface {
	RequestTask() (RequestTaskResponse, error)
	CompleteTask(taskID string) error
	ResetTask(taskID string) error
}

func RunWorkerRPC(client RemoteCoordinator, dir string, needle string, load ContentProvider) error {
	for {
		resp, err := client.RequestTask()
		if err != nil {
			return err
		}

		if resp.Done || resp.Wait {
			return nil
		}

		contents, err := load(resp.Task.Input)
		if err != nil {
			return err
		}

		if err := ExecuteAssignedMapTask(resp.Task, dir, needle, contents); err != nil {
			return err
		}

		if err := client.CompleteTask(resp.Task.ID); err != nil {
			return err
		}
	}
}
