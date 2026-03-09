package mr

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestTaskStart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		initial    TaskStatus
		want       TaskStatus
		wantErr    error
		wantErrMsg string
	}{
		{
			name:    "start from pending",
			initial: TaskStatusPending,
			want:    TaskStatusRunning,
		},
		{
			name:       "start from running",
			initial:    TaskStatusRunning,
			want:       TaskStatusRunning,
			wantErr:    ErrTaskCannotStart,
			wantErrMsg: "cannot start task from status running",
		},
		{
			name:       "start from done",
			initial:    TaskStatusDone,
			want:       TaskStatusDone,
			wantErr:    ErrTaskCannotStart,
			wantErrMsg: "cannot start task from status done",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			task := Task{Status: tt.initial}
			err := task.Start()

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Start() error = %v, want %v", err, tt.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), tt.wantErrMsg) {
				t.Fatalf("Start() error message = %q, want substring %q", err.Error(), tt.wantErrMsg)
			}
			if task.Status != tt.want {
				t.Fatalf("task.Status = %s, want %s", task.Status, tt.want)
			}
		})
	}
}

func TestTaskComplete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		initial    TaskStatus
		want       TaskStatus
		wantErr    error
		wantErrMsg string
	}{
		{
			name:    "complete from running",
			initial: TaskStatusRunning,
			want:    TaskStatusDone,
		},
		{
			name:       "complete from pending",
			initial:    TaskStatusPending,
			want:       TaskStatusPending,
			wantErr:    ErrTaskCannotComplete,
			wantErrMsg: "cannot complete task from status pending",
		},
		{
			name:       "complete from done",
			initial:    TaskStatusDone,
			want:       TaskStatusDone,
			wantErr:    ErrTaskCannotComplete,
			wantErrMsg: "cannot complete task from status done",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			task := Task{Status: tt.initial}
			err := task.Complete()

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Complete() error = %v, want %v", err, tt.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), tt.wantErrMsg) {
				t.Fatalf("Complete() error message = %q, want substring %q", err.Error(), tt.wantErrMsg)
			}
			if task.Status != tt.want {
				t.Fatalf("task.Status = %s, want %s", task.Status, tt.want)
			}
		})
	}
}

func TestTaskReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		initial    TaskStatus
		want       TaskStatus
		wantErr    error
		wantErrMsg string
	}{
		{
			name:    "reset from running",
			initial: TaskStatusRunning,
			want:    TaskStatusPending,
		},
		{
			name:       "reset from pending",
			initial:    TaskStatusPending,
			want:       TaskStatusPending,
			wantErr:    ErrTaskCannotReset,
			wantErrMsg: "cannot reset task from status pending",
		},
		{
			name:       "reset from done",
			initial:    TaskStatusDone,
			want:       TaskStatusDone,
			wantErr:    ErrTaskCannotReset,
			wantErrMsg: "cannot reset task from status done",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			task := Task{Status: tt.initial}
			err := task.Reset()

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Reset() error = %v, want %v", err, tt.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), tt.wantErrMsg) {
				t.Fatalf("Reset() error message = %q, want substring %q", err.Error(), tt.wantErrMsg)
			}
			if task.Status != tt.want {
				t.Fatalf("task.Status = %s, want %s", task.Status, tt.want)
			}
		})
	}
}

func TestTaskLifecycleHappyPath(t *testing.T) {
	t.Parallel()

	task := Task{Status: TaskStatusPending}

	if err := task.Start(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	if err := task.Complete(); err != nil {
		t.Fatalf("Complete() error = %v, want nil", err)
	}
	if task.Status != TaskStatusDone {
		t.Fatalf("task.Status = %s, want %s", task.Status, TaskStatusDone)
	}
	if !task.IsTerminal() {
		t.Fatalf("IsTerminal() = false, want true")
	}
}

func TestTaskLifecycleReassignmentPath(t *testing.T) {
	t.Parallel()

	task := Task{Status: TaskStatusPending}

	if err := task.Start(); err != nil {
		t.Fatalf("first Start() error = %v, want nil", err)
	}
	if err := task.Reset(); err != nil {
		t.Fatalf("Reset() error = %v, want nil", err)
	}
	if err := task.Start(); err != nil {
		t.Fatalf("second Start() error = %v, want nil", err)
	}
	if err := task.Complete(); err != nil {
		t.Fatalf("Complete() error = %v, want nil", err)
	}
	if task.Status != TaskStatusDone {
		t.Fatalf("task.Status = %s, want %s", task.Status, TaskStatusDone)
	}
}

func TestTaskStartLeaseSetsExpiry(t *testing.T) {
	t.Parallel()

	task := Task{Status: TaskStatusPending}
	startedAt := time.Date(2026, time.March, 9, 12, 0, 0, 0, time.UTC)

	if err := task.StartLease(startedAt, 5*time.Second); err != nil {
		t.Fatalf("StartLease() error = %v, want nil", err)
	}

	if task.LeaseExpiresAt != startedAt.Add(5*time.Second) {
		t.Fatalf("task.LeaseExpiresAt = %v, want %v", task.LeaseExpiresAt, startedAt.Add(5*time.Second))
	}
}

func TestTaskLeaseExpired(t *testing.T) {
	t.Parallel()

	expiresAt := time.Date(2026, time.March, 9, 12, 0, 5, 0, time.UTC)
	task := Task{
		Status:         TaskStatusRunning,
		LeaseExpiresAt: expiresAt,
	}

	if !task.LeaseExpired(expiresAt) {
		t.Fatal("LeaseExpired() = false at deadline, want true")
	}
	if task.LeaseExpired(expiresAt.Add(-time.Nanosecond)) {
		t.Fatal("LeaseExpired() = true before deadline, want false")
	}
}
