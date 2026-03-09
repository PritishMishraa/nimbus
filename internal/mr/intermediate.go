package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"nimbus/internal/apps"
)

var (
	ErrEmptyTaskID = errors.New("task id is empty")
	ErrEmptyDir    = errors.New("dir is empty")
)

func IntermediateFileName(taskID string) string {
	return "mr-" + taskID + ".jsonl"
}

func WriteIntermediate(taskID string, dir string, kvs []apps.KeyValue) (string, error) {
	if taskID == "" {
		return "", ErrEmptyTaskID
	}

	if dir == "" {
		return "", ErrEmptyDir
	}

	path := filepath.Join(dir, IntermediateFileName(taskID))

	file, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("create intermediate file %q: %w", path, err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for _, kv := range kvs {
		if err := encoder.Encode(kv); err != nil {
			return "", fmt.Errorf("encode intermediate record to %q: %w", path, err)
		}
	}

	return path, nil
}
