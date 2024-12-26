package utils

import "strings"

// ReplaceHyphenWithUnderscore replaces all occurrences of '-' with '_' in the given string.
func ReplaceHyphenWithUnderscore(instID string) string {
	return strings.ReplaceAll(instID, "-", "_")
}
