package sql

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jansemmelink/j2/data"
	"github.com/jansemmelink/log"
	jsql "github.com/jansemmelink/sql"
)

func TestDb(t *testing.T) {
	log.DebugOn()
	log.Debugf("Testing...")
	db, err := New(jsql.Connection{
		User:     "store_api",
		Pass:     "store",
		Database: "store",
	})
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	if err := data.RunDbTests(db); err != nil {
		t.Fatalf("db tests failed: %v", err)
	}
}
