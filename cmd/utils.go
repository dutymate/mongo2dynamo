package cmd

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

// confirm prompts the user for confirmation and returns true if the user confirms.
func confirm(prompt string) bool {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(prompt)
	response, err := reader.ReadString('\n')
	if err != nil {
		return false
	}
	response = strings.ToLower(strings.TrimSpace(response))
	return response == "y" || response == "yes"
}

func Confirm(prompt string) bool {
	return ConfirmWithReader(os.Stdin, prompt)
}

func ConfirmWithReader(r io.Reader, prompt string) bool {
	fmt.Print(prompt)
	reader := bufio.NewReader(r)
	text, _ := reader.ReadString('\n')
	return strings.TrimSpace(text) == "yes"
}
