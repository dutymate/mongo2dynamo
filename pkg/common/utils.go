package common

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// Confirm prompts the user for confirmation and returns true if the user confirms.
func Confirm(prompt string) bool {
	return ConfirmWithReader(os.Stdin, prompt)
}

// ConfirmWithReader prompts the user for confirmation using the provided reader and returns true if the user confirms.
func ConfirmWithReader(r io.Reader, prompt string) bool {
	fmt.Print(prompt)
	reader := bufio.NewReader(r)
	text, err := reader.ReadString('\n')
	if err != nil {
		return false
	}
	text = strings.ToLower(strings.TrimSpace(text))
	return text == "y" || text == "yes"
}

// FormatNumber formats an integer with comma separators for thousands.
func FormatNumber(n int) string {
	if n == 0 {
		return "0"
	}

	negative := n < 0
	if negative {
		n = -n
	}

	var parts []string
	for n > 0 {
		parts = append(parts, strconv.Itoa(n%1000))
		n /= 1000
	}

	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}

	for i := 1; i < len(parts); i++ {
		if len(parts[i]) < 3 {
			parts[i] = strings.Repeat("0", 3-len(parts[i])) + parts[i]
		}
	}

	var b strings.Builder
	if negative {
		b.WriteByte('-')
	}
	for i, part := range parts {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(part)
	}
	return b.String()
}
