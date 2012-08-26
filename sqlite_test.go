package sqlite

import (
	"database/sql"
	"testing"
)

func assert(t *testing.T, s bool, args ...interface{}) {
	if !s {
		t.Error(args...)
	}
}

func TestDriver(t *testing.T) {
	db, err := sql.Open("sqlite", "testdb")
	defer db.Close()

	assert(t, db != nil, "db is nil")
	assert(t, err == nil, "sql.Open err", err)

	result, err := db.Exec("create table testtable (id integer primary key, name text);")
	assert(t, result != nil, "result is nil")
	assert(t, err == nil, "db.Exec err", err)

	defer db.Exec("drop table testtable;")

	const n = 3
	for i := 0; i < n; i++ {
		result, err = db.Exec("insert into testtable (id, name) values (?, ?);", i*100, "hogehoge")
		assert(t, result != nil, "result is nil")
		nrows, err := result.RowsAffected()
		assert(t, nrows == 1, "invalid rows affected", nrows)
		assert(t, err == nil, "db.Exec err", err)
	}

	rows, err := db.Query("select id, name from testtable order by id asc;")
	assert(t, rows != nil)
	assert(t, err == nil, "db.Query err", err)
	defer rows.Close()

	cols, err := rows.Columns()
	assert(t, len(cols) == 2, "invalid cols length", len(cols))
	assert(t, cols[0] == "id" && cols[1] == "name", "invalid column names", cols)

	i := 0
	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		assert(t, id == i*100, "invalid id value", id)
		assert(t, name == "hogehoge", "invalid name value", name)
		i++
	}

	assert(t, i == n)
}
