package mr

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"nimbus/internal/apps"
)

func TestReadIntermediateValid(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	want := []apps.KeyValue{
		{Key: "notes.txt", Value: "2:beta match"},
		{Key: "notes.txt", Value: "4:match delta"},
		{Key: "other.txt", Value: "1:first match"},
	}

	path, err := WriteIntermediate("task-1", dir, want)
	if err != nil {
		t.Fatalf("WriteIntermediate() error = %v, want nil", err)
	}

	got, err := ReadIntermediate(path)
	if err != nil {
		t.Fatalf("ReadIntermediate() error = %v, want nil", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadIntermediate() = %#v, want %#v", got, want)
	}
}

func TestReadIntermediateEmptyFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	path, err := WriteIntermediate("task-1", dir, []apps.KeyValue{})
	if err != nil {
		t.Fatalf("WriteIntermediate() error = %v, want nil", err)
	}

	got, err := ReadIntermediate(path)
	if err != nil {
		t.Fatalf("ReadIntermediate() error = %v, want nil", err)
	}

	if len(got) != 0 {
		t.Fatalf("ReadIntermediate() len = %d, want 0", len(got))
	}
}

func TestReadIntermediateRejectsEmptyPath(t *testing.T) {
	t.Parallel()

	_, err := ReadIntermediate("")
	if !errors.Is(err, ErrEmptyPath) {
		t.Fatalf("ReadIntermediate() error = %v, want %v", err, ErrEmptyPath)
	}
}

func TestReadIntermediateInvalidJSONReturnsError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, IntermediateFileName("task-1"))

	if err := os.WriteFile(path, []byte("{\"key\":\"notes.txt\"\n"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%q) error = %v, want nil", path, err)
	}

	_, err := ReadIntermediate(path)
	if err == nil {
		t.Fatal("ReadIntermediate() error = nil, want non-nil")
	}
}

func TestGroupIntermediateByKey(t *testing.T) {
	t.Parallel()

	records := []apps.KeyValue{
		{Key: "a.txt", Value: "1:hello"},
		{Key: "a.txt", Value: "3:again"},
		{Key: "b.txt", Value: "2:world"},
	}

	got := GroupIntermediateByKey(records)
	want := map[string][]string{
		"a.txt": {"1:hello", "3:again"},
		"b.txt": {"2:world"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GroupIntermediateByKey() = %#v, want %#v", got, want)
	}
}

func TestGroupIntermediateByKeyPreservesPerKeyOrder(t *testing.T) {
	t.Parallel()

	records := []apps.KeyValue{
		{Key: "notes.txt", Value: "3:third"},
		{Key: "other.txt", Value: "1:first"},
		{Key: "notes.txt", Value: "7:seventh"},
		{Key: "notes.txt", Value: "8:eighth"},
		{Key: "other.txt", Value: "2:second"},
	}

	got := GroupIntermediateByKey(records)

	if !reflect.DeepEqual(got["notes.txt"], []string{"3:third", "7:seventh", "8:eighth"}) {
		t.Fatalf("grouped notes.txt = %#v, want %#v", got["notes.txt"], []string{"3:third", "7:seventh", "8:eighth"})
	}

	if !reflect.DeepEqual(got["other.txt"], []string{"1:first", "2:second"}) {
		t.Fatalf("grouped other.txt = %#v, want %#v", got["other.txt"], []string{"1:first", "2:second"})
	}
}

func TestExecuteMapTaskReadAndGroupIntermediateMiniIntegration(t *testing.T) {
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

	records, err := ReadIntermediate(path)
	if err != nil {
		t.Fatalf("ReadIntermediate() error = %v, want nil", err)
	}

	got := GroupIntermediateByKey(records)
	want := map[string][]string{
		"notes.txt": {"1:first hit", "3:third hit"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("grouped records = %#v, want %#v", got, want)
	}
}
