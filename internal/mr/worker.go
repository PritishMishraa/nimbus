package mr

import "errors"

type ContentProvider func(input string) (string, error)

func RunWorker(c *Coordinator, dir string, needle string, load ContentProvider) error {
	for {
		task, err := c.NextTask()
		if err != nil {
			if errors.Is(err, ErrAllTasksDone) {
				return nil
			}

			return err
		}

		contents, err := load(task.Input)
		if err != nil {
			return err
		}

		if err := RunTask(c, task.ID, dir, needle, contents); err != nil {
			return err
		}
	}
}
