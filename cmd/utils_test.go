package cmd

import (
	"bytes"
	"testing"
)

func TestConfirmWithReader_yes(t *testing.T) {
	input := bytes.NewBufferString("yes\n")
	result := ConfirmWithReader(input, "Proceed? ")
	if !result {
		t.Errorf("expected true, got false")
	}
}

func TestConfirmWithReader_no(t *testing.T) {
	input := bytes.NewBufferString("no\n")
	result := ConfirmWithReader(input, "Proceed? ")
	if result {
		t.Errorf("expected false, got true")
	}
}
