package db

import (
	"testing"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

func Test_Repro1658(t *testing.T) {
	db, path := mustCreateOnDiskDatabaseWAL()
	defer fsutil.Remove(path)
	defer db.Close()
	if _, err := db.ExecuteStringStmt("CREATE TABLE sample (start_time datetime)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecuteStringStmt("INSERT INTO sample VALUES('2024-01-31-05:00')"); err != nil {
		t.Fatal(err)
	}
	r, err := db.QueryStringStmt("SELECT * FROM sample")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s", asJSON(r))
}
