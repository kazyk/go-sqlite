package sqlite

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
)

func assert(t *testing.T, s bool, args ...interface{}) {
	if !s {
		_, file, line, ok := runtime.Caller(1)
		if ok {
			file = filepath.Base(file)
			msg := fmt.Sprintf("failed in %v:%v", file, line)
			a := make([]interface{}, 0)
			a = append(a, msg)
			a = append(a, args...)
			t.Error(a)
		} else {
			t.Error(args...)
		}
	}
}

// rough tests

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

func TestTransactionRollback(t *testing.T) {
	db, _ := sql.Open("sqlite", "testdb")
	defer db.Close()
	_, err := db.Exec("create table testtx (id integer primary key);")
	assert(t, err == nil)
	defer db.Exec("drop table testtx;")

	db.Exec("insert into testtx (id) values (100);")
	row := db.QueryRow("select * from testtx;")
	var id int
	row.Scan(&id)
	assert(t, id == 100)

	{
		tx, err := db.Begin()
		assert(t, tx != nil)
		assert(t, err == nil)

		_, err = tx.Exec("update testtx set id = 1000;")
		assert(t, err == nil)
		row := tx.QueryRow("select * from testtx;")
		var id int
		row.Scan(&id)
		assert(t, id == 1000)
		err = tx.Rollback()
		assert(t, err == nil)
	}

	{
		db,_ := sql.Open("sqlite", "testdb")
		defer db.Close()
		row := db.QueryRow("select * from testtx;")
		var id int
		row.Scan(&id)
		assert(t, id == 100)
	}
}

func TestTransactionCommit(t *testing.T) {
	db, _ := sql.Open("sqlite", "testdb")
	defer db.Close()

	_, err := db.Exec("create table testtx (id integer primary key);")
	assert(t, err == nil)
	defer db.Exec("drop table testtx;")

	db.Exec("insert into testtx (id) values (100);")
	row := db.QueryRow("select * from testtx;")
	var id int
	row.Scan(&id)
	assert(t, id == 100)

	{
		tx, err := db.Begin()
		assert(t, tx != nil)
		assert(t, err == nil)

		_, err = tx.Exec("update testtx set id = 1000;")
		assert(t, err == nil)
		row := tx.QueryRow("select * from testtx;")
		var id int
		row.Scan(&id)
		assert(t, id == 1000)
		err = tx.Commit()
		assert(t, err == nil)
	}

	{
		db,_ := sql.Open("sqlite", "testdb")
		defer db.Close()
		row := db.QueryRow("select * from testtx;")
		var id int
		row.Scan(&id)
		assert(t, id == 1000, "unexpected id:", id)
	}
}

func TestTypes(t *testing.T) {
	db, _ := sql.Open("sqlite", "testdb")
	defer db.Close()

	_, err := db.Exec(`create table testtypes (
		id integer,
		name text,
		nil text,
		fl float);`)
	assert(t, err == nil)
	defer db.Exec("drop table testtypes;")

	_, err = db.Exec("insert into testtypes (id, name, nil, fl) values (?, ?, ?, ?);",
		100, "testname", nil, 10.11)
	assert(t, err == nil)

	row := db.QueryRow("select * from testtypes;")
	var id int
	var name string
	var nnil interface{}
	var fl float64
	row.Scan(&id, &name, &nnil, &fl)
	assert(t, id == 100, "unexpected id:", id)
	assert(t, name == "testname", "unexpected name:", name)
	assert(t, nnil == nil)
	assert(t, fl == 10.11, "unexpected fl:", fl)
}
