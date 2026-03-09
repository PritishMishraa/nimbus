package mr

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"nimbus/internal/apps"
)

func TestIntermediateFileNameDeterministic(t *testing.T) {
	t.Parallel()

	got := IntermediateFileName("task-1")
	want := "mr-task-1.jsonl"
	if got != want {
		t.Fatalf("IntermediateFileName() = %q, want %q", got, want)
	}
}

func TestWriteIntermediateValid(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	kvs := []apps.KeyValue{
		{Key: "notes.txt", Value: "2:beta match"},
		{Key: "notes.txt", Value: "4:match delta"},
		{Key: "other.txt", Value: "1:first match"},
	}

	gotPath, err := WriteIntermediate("task-1", dir, kvs)
	if err != nil {
		t.Fatalf("WriteIntermediate() error = %v, want nil", err)
	}

	wantPath := filepath.Join(dir, IntermediateFileName("task-1"))
	if gotPath != wantPath {
		t.Fatalf("WriteIntermediate() path = %q, want %q", gotPath, wantPath)
	}

	if _, err := os.Stat(gotPath); err != nil {
		t.Fatalf("os.Stat(%q) error = %v, want nil", gotPath, err)
	}

	gotRecords := readIntermediateRecords(t, gotPath)
	if !reflect.DeepEqual(gotRecords, kvs) {
		t.Fatalf("decoded records = %#v, want %#v", gotRecords, kvs)
	}
}

func TestWriteIntermediateRejectsEmptyTaskID(t *testing.T) {
	t.Parallel()

	_, err := WriteIntermediate("", t.TempDir(), nil)
	if !errors.Is(err, ErrEmptyTaskID) {
		t.Fatalf("WriteIntermediate() error = %v, want %v", err, ErrEmptyTaskID)
	}
}

func TestWriteIntermediateRejectsEmptyDir(t *testing.T) {
	t.Parallel()

	_, err := WriteIntermediate("task-1", "", nil)
	if !errors.Is(err, ErrEmptyDir) {
		t.Fatalf("WriteIntermediate() error = %v, want %v", err, ErrEmptyDir)
	}
}

func TestWriteIntermediateEmptyKeyValuesIsValid(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	gotPath, err := WriteIntermediate("task-1", dir, []apps.KeyValue{})
	if err != nil {
		t.Fatalf("WriteIntermediate() error = %v, want nil", err)
	}

	if _, err := os.Stat(gotPath); err != nil {
		t.Fatalf("os.Stat(%q) error = %v, want nil", gotPath, err)
	}

	gotRecords := readIntermediateRecords(t, gotPath)
	if len(gotRecords) != 0 {
		t.Fatalf("decoded records len = %d, want 0", len(gotRecords))
	}
}

func TestWriteIntermediatePathDeterministic(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	firstPath, err := WriteIntermediate("task-1", dir, nil)
	if err != nil {
		t.Fatalf("first WriteIntermediate() error = %v, want nil", err)
	}

	secondPath, err := WriteIntermediate("task-1", dir, nil)
	if err != nil {
		t.Fatalf("second WriteIntermediate() error = %v, want nil", err)
	}

	if firstPath != secondPath {
		t.Fatalf("WriteIntermediate() paths = %q and %q, want equal", firstPath, secondPath)
	}
}

func TestExecuteMapTaskWriteIntermediateMiniIntegration(t *testing.T) {
	t.Parallel()

	task := Task{
		ID:     "task-42",
		Type:   TaskTypeMap,
		Input:  "notes.txt",
		Status: TaskStatusPending,
	}

	contents := "first hit\nskip me\nthird hit\nanother skip"
	needle := "hit"

	kvs, err := ExecuteMapTask(task, needle, contents)
	if err != nil {
		t.Fatalf("ExecuteMapTask() error = %v, want nil", err)
	}

	dir := t.TempDir()
	path, err := WriteIntermediate(task.ID, dir, kvs)
	if err != nil {
		t.Fatalf("WriteIntermediate() error = %v, want nil", err)
	}

	gotRecords := readIntermediateRecords(t, path)
	if !reflect.DeepEqual(gotRecords, kvs) {
		t.Fatalf("decoded records = %#v, want %#v", gotRecords, kvs)
	}
}

func readIntermediateRecords(t *testing.T, path string) []apps.KeyValue {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("os.Open(%q) error = %v, want nil", path, err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	records := make([]apps.KeyValue, 0)

	for {
		var kv apps.KeyValue
		err := decoder.Decode(&kv)
		if errors.Is(err, io.EOF) {
			return records
		}
		if err != nil {
			t.Fatalf("decoder.Decode() error = %v, want nil", err)
		}

		records = append(records, kv)
	}
}
