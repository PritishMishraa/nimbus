package mr

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestExecuteReduceEmptyGroupedInput(t *testing.T) {
	t.Parallel()

	if got := ExecuteReduce(nil); got != "" {
		t.Fatalf("ExecuteReduce(nil) = %q, want empty string", got)
	}

	if got := ExecuteReduce(map[string][]string{}); got != "" {
		t.Fatalf("ExecuteReduce(empty) = %q, want empty string", got)
	}
}

func TestExecuteReduceSingleKey(t *testing.T) {
	t.Parallel()

	grouped := map[string][]string{
		"notes.txt": {"2:beta match", "4:match delta"},
	}

	got := ExecuteReduce(grouped)
	want := "notes.txt\n2:beta match\n4:match delta"
	if got != want {
		t.Fatalf("ExecuteReduce() = %q, want %q", got, want)
	}
}

func TestExecuteReduceMultipleKeysDeterministic(t *testing.T) {
	t.Parallel()

	grouped := map[string][]string{
		"b.txt": {"2:world"},
		"a.txt": {"3:again", "1:hello"},
	}

	got := ExecuteReduce(grouped)
	want := "a.txt\n1:hello\n3:again\n\nb.txt\n2:world"
	if got != want {
		t.Fatalf("ExecuteReduce() = %q, want %q", got, want)
	}
}

func TestFinalOutputFileNameDeterministic(t *testing.T) {
	t.Parallel()

	got := FinalOutputFileName("task-1")
	want := "out-task-1.txt"
	if got != want {
		t.Fatalf("FinalOutputFileName() = %q, want %q", got, want)
	}
}

func TestWriteFinalOutputSuccess(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	output := "notes.txt\n1:first hit\n3:third hit"

	gotPath, err := WriteFinalOutput("task-1", dir, output)
	if err != nil {
		t.Fatalf("WriteFinalOutput() error = %v, want nil", err)
	}

	wantPath := filepath.Join(dir, FinalOutputFileName("task-1"))
	if gotPath != wantPath {
		t.Fatalf("WriteFinalOutput() path = %q, want %q", gotPath, wantPath)
	}

	gotBytes, err := os.ReadFile(gotPath)
	if err != nil {
		t.Fatalf("os.ReadFile(%q) error = %v, want nil", gotPath, err)
	}

	if string(gotBytes) != output {
		t.Fatalf("final output contents = %q, want %q", string(gotBytes), output)
	}
}

func TestWriteFinalOutputRejectsEmptyTaskID(t *testing.T) {
	t.Parallel()

	_, err := WriteFinalOutput("", t.TempDir(), "output")
	if !errors.Is(err, ErrEmptyTaskID) {
		t.Fatalf("WriteFinalOutput() error = %v, want %v", err, ErrEmptyTaskID)
	}
}

func TestWriteFinalOutputRejectsEmptyDir(t *testing.T) {
	t.Parallel()

	_, err := WriteFinalOutput("task-1", "", "output")
	if !errors.Is(err, ErrEmptyDir) {
		t.Fatalf("WriteFinalOutput() error = %v, want %v", err, ErrEmptyDir)
	}
}

func TestGroupedReduceWriteMiniIntegration(t *testing.T) {
	t.Parallel()

	grouped := map[string][]string{
		"b.txt": {"2:world"},
		"a.txt": {"1:hello", "3:again"},
	}

	output := ExecuteReduce(grouped)

	dir := t.TempDir()
	path, err := WriteFinalOutput("task-1", dir, output)
	if err != nil {
		t.Fatalf("WriteFinalOutput() error = %v, want nil", err)
	}

	gotBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile(%q) error = %v, want nil", path, err)
	}

	want := "a.txt\n1:hello\n3:again\n\nb.txt\n2:world"
	if string(gotBytes) != want {
		t.Fatalf("final output contents = %q, want %q", string(gotBytes), want)
	}
}

func TestMapIntermediateReadGroupReduceWriteIntegration(t *testing.T) {
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
	intermediatePath, err := WriteIntermediate(task.ID, dir, kvs)
	if err != nil {
		t.Fatalf("WriteIntermediate() error = %v, want nil", err)
	}

	records, err := ReadIntermediate(intermediatePath)
	if err != nil {
		t.Fatalf("ReadIntermediate() error = %v, want nil", err)
	}

	grouped := GroupIntermediateByKey(records)
	output := ExecuteReduce(grouped)

	finalPath, err := WriteFinalOutput(task.ID, dir, output)
	if err != nil {
		t.Fatalf("WriteFinalOutput() error = %v, want nil", err)
	}

	gotBytes, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("os.ReadFile(%q) error = %v, want nil", finalPath, err)
	}

	want := "notes.txt\n1:first hit\n3:third hit"
	if string(gotBytes) != want {
		t.Fatalf("final output contents = %q, want %q", string(gotBytes), want)
	}
}
