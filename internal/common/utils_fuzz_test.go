package common

import "testing"

func FuzzFormatNumber(f *testing.F) {
	// Seed corpus with various representative values.
	f.Add(0)
	f.Add(1)
	f.Add(-1)
	f.Add(100)
	f.Add(-100)
	f.Add(1000000)
	f.Add(-1000000)
	f.Add(int(^uint(0) >> 1))    // max int.
	f.Add(-int(^uint(0)>>1) - 1) // min int.

	f.Fuzz(func(_ *testing.T, n int) {
		_ = FormatNumber(n)
	})
}
