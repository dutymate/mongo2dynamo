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
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(prompt)
	response, err := reader.ReadString('\n')
	if err != nil {
		return false
	}
	response = strings.ToLower(strings.TrimSpace(response))
	return response == "y" || response == "yes"
}

func ConfirmWithReader(r io.Reader, prompt string) bool {
	fmt.Print(prompt)
	reader := bufio.NewReader(r)
	text, _ := reader.ReadString('\n')
	text = strings.ToLower(strings.TrimSpace(text))
	return text == "y" || text == "yes"
}
