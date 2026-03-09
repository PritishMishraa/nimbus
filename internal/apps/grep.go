package apps

import (
	"sort"
	"strconv"
	"strings"
)

// GrepApp names the placeholder app that will eventually host grep-specific logic.
const GrepApp = "grep"

// KeyValue is the minimal application-level contract for map output.
type KeyValue struct {
	Key   string
	Value string
}

// MapGrep scans file contents line by line and emits one record per substring match.
func MapGrep(filename string, contents string, needle string) []KeyValue {
	if contents == "" {
		return []KeyValue{}
	}

	lines := strings.Split(contents, "\n")
	matches := make([]KeyValue, 0)

	for i, line := range lines {
		if strings.Contains(line, needle) {
			matches = append(matches, KeyValue{
				Key:   filename,
				Value: strconv.Itoa(i+1) + ":" + line,
			})
		}
	}

	return matches
}

// ReduceGrep formats all matches for a single file into one deterministic string.
func ReduceGrep(key string, values []string) string {
	if len(values) == 0 {
		return key
	}

	sortedValues := append([]string(nil), values...)
	sort.Slice(sortedValues, func(i, j int) bool {
		leftLine, leftOK := parseLineNumber(sortedValues[i])
		rightLine, rightOK := parseLineNumber(sortedValues[j])

		switch {
		case leftOK && rightOK && leftLine != rightLine:
			return leftLine < rightLine
		case leftOK != rightOK:
			return leftOK
		default:
			return sortedValues[i] < sortedValues[j]
		}
	})

	return key + "\n" + strings.Join(sortedValues, "\n")
}

func parseLineNumber(value string) (int, bool) {
	line, _, found := strings.Cut(value, ":")
	if !found {
		return 0, false
	}

	lineNumber, err := strconv.Atoi(line)
	if err != nil {
		return 0, false
	}

	return lineNumber, true
}
