package items

import (
	"testing"
)

func TestDb(t *testing.T) {
	if err := RunDbTests(New("db")); err != nil {
		t.Fatalf("failed: %v", err)
	}
}
