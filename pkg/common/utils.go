package common

import (
	"bufio"
	"fmt"
	"io"
	"os"
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
