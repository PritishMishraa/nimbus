package apps

import (
	"reflect"
	"testing"
)

func TestMapGrep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filename string
		contents string
		needle   string
		want     []KeyValue
	}{
		{
			name:     "single match",
			filename: "notes.txt",
			contents: "hello\nworld",
			needle:   "hello",
			want: []KeyValue{
				{Key: "notes.txt", Value: "1:hello"},
			},
		},
		{
			name:     "multiple matches in same file",
			filename: "notes.txt",
			contents: "hello\nworld\nhello again",
			needle:   "hello",
			want: []KeyValue{
				{Key: "notes.txt", Value: "1:hello"},
				{Key: "notes.txt", Value: "3:hello again"},
			},
		},
		{
			name:     "no match",
			filename: "notes.txt",
			contents: "hello\nworld",
			needle:   "absent",
			want:     []KeyValue{},
		},
		{
			name:     "empty contents",
			filename: "notes.txt",
			contents: "",
			needle:   "hello",
			want:     []KeyValue{},
		},
		{
			name:     "substring match",
			filename: "notes.txt",
			contents: "hello world\ngoodbye",
			needle:   "hell",
			want: []KeyValue{
				{Key: "notes.txt", Value: "1:hello world"},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := MapGrep(tt.filename, tt.contents, tt.needle)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("MapGrep() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestReduceGrep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		key    string
		values []string
		want   string
	}{
		{
			name:   "multiple values sorted deterministically",
			key:    "notes.txt",
			values: []string{"3:hello again", "1:hello"},
			want:   "notes.txt\n1:hello\n3:hello again",
		},
		{
			name:   "empty values returns filename header",
			key:    "notes.txt",
			values: []string{},
			want:   "notes.txt",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ReduceGrep(tt.key, tt.values)
			if got != tt.want {
				t.Fatalf("ReduceGrep() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMapReduceGrepFlow(t *testing.T) {
	t.Parallel()

	mapped := MapGrep("notes.txt", "hello\nworld\nhello again", "hello")

	grouped := make(map[string][]string)
	for _, kv := range mapped {
		grouped[kv.Key] = append(grouped[kv.Key], kv.Value)
	}

	got := ReduceGrep("notes.txt", grouped["notes.txt"])
	want := "notes.txt\n1:hello\n3:hello again"

	if got != want {
		t.Fatalf("full flow output = %q, want %q", got, want)
	}
}
