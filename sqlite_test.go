package sqlite

import (
	"database/sql"
	"testing"
)

func TestDriver(t *testing.T) {
	db, err := sql.Open("sqlite", "testdb")
	defer db.Close()

	if db == nil {
		t.Error("db is nil")
	}
	if err != nil {
		t.Error("sql.Open err", err)
	}

	result, err := db.Exec("create table testtable (id integer primary key, name text);")
	if result == nil {
		t.Error("result is nil")
	}
	if err != nil {
		t.Error("db.Exec err", err)
	}

	defer db.Exec("drop table testtable;")

	const n = 3
	for i := 0; i < n; i++ {
		result, err = db.Exec("insert into testtable (id, name) values (?, ?);", i*100, "hogehoge")
		if result == nil {
			t.Error("result is nil")
		}
		nrows, err := result.RowsAffected()
		if nrows != 1 {
			t.Error("invalid rows affected", nrows)
		}
		if err != nil {
			t.Error("db.Exec err", err)
		}
	}

	rows, err := db.Query("select id, name from testtable order by id asc;")
	if rows == nil {
		t.Error("rows is nil")
	}
	if err != nil {
		t.Error("db.Query err", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if len(cols) != 2 {
		t.Error("cols")
	}
	if cols[0] != "id" || cols[1] != "name" {
		t.Error("invalid columns", cols)
	}

	i := 0
	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		if id != i*100 {
			t.Error("invalid id value", id)
		}
		if name != "hogehoge" {
			t.Error("invalid name value", name)
		}
		i++
	}

	if i != n {
		t.Error()
	}
}
