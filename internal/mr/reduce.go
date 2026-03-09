package mr

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"nimbus/internal/apps"
)

func ExecuteReduce(grouped map[string][]string) string {
	if len(grouped) == 0 {
		return ""
	}

	keys := make([]string, 0, len(grouped))
	for key := range grouped {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	blocks := make([]string, 0, len(keys))
	for _, key := range keys {
		blocks = append(blocks, apps.ReduceGrep(key, grouped[key]))
	}

	var builder strings.Builder
	for i, block := range blocks {
		if i > 0 {
			builder.WriteString("\n\n")
		}
		builder.WriteString(block)
	}

	return builder.String()
}

func FinalOutputFileName(taskID string) string {
	return "out-" + taskID + ".txt"
}

func WriteFinalOutput(taskID string, dir string, output string) (string, error) {
	if taskID == "" {
		return "", ErrEmptyTaskID
	}

	if dir == "" {
		return "", ErrEmptyDir
	}

	path := filepath.Join(dir, FinalOutputFileName(taskID))
	if err := os.WriteFile(path, []byte(output), 0o644); err != nil {
		return "", fmt.Errorf("write final output file %q: %w", path, err)
	}

	return path, nil
}
