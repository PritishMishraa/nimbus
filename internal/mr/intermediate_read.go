package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"nimbus/internal/apps"
)

var ErrEmptyPath = errors.New("path is empty")

func ReadIntermediate(path string) ([]apps.KeyValue, error) {
	if path == "" {
		return nil, ErrEmptyPath
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open intermediate file %q: %w", path, err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	records := make([]apps.KeyValue, 0)

	for {
		var kv apps.KeyValue
		err := decoder.Decode(&kv)
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return nil, fmt.Errorf("decode intermediate record from %q: %w", path, err)
		}

		records = append(records, kv)
	}
}

func GroupIntermediateByKey(records []apps.KeyValue) map[string][]string {
	grouped := make(map[string][]string, len(records))

	for _, record := range records {
		grouped[record.Key] = append(grouped[record.Key], record.Value)
	}

	return grouped
}
