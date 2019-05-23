package jsonfiles_test

import (
	"testing"

	"github.com/jansemmelink/items/jsonfiles"
	"github.com/jansemmelink/items"
	"github.com/jansemmelink/log"
)

func TestDb(t *testing.T) {
	log.DebugOn()
	log.Debugf("Testing...")
	db, err := jsonfiles.New("store")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	if err := items.RunDbTests(db); err != nil {
		t.Fatalf("db tests failed: %v", err)
	}
}
