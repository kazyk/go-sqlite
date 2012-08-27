package sqlite

/*
#cgo LDFLAGS: -lsqlite3
#include <sqlite3.h>
#include <stdlib.h>
#include <strings.h>

int bind_text(sqlite3_stmt *stmt, int i, const char *str, int n) {
	return sqlite3_bind_text(stmt, i, str, n, SQLITE_TRANSIENT);
}
*/
import "C"
import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"unsafe"
)

func init() {
	sql.Register("sqlite", Driver{})
}

type Driver struct {
}

func (Driver) Open(name string) (driver.Conn, error) {
	n := C.CString(name)
	defer C.free(unsafe.Pointer(n))

	var conn Conn
	r := C.sqlite3_open(n, &conn.db)
	if r != C.SQLITE_OK {
		return &conn, driver.ErrBadConn
	}
	return &conn, nil
}

type Conn struct {
	db *C.sqlite3
}

func (conn Conn) Prepare(query string) (driver.Stmt, error) {
	s := C.CString(query)
	defer C.free(unsafe.Pointer(s))
	l := C.int(C.strlen(s))

	var stmt Stmt
	var tail *C.char
	r := C.sqlite3_prepare_v2(conn.db, s, l, &stmt.stmt, &tail)

	if r != C.SQLITE_OK {
		return &stmt, dbError(conn.db)
	}
	return &stmt, nil
}

func (conn Conn) Close() error {
	r := C.sqlite3_close(conn.db)
	if r != C.SQLITE_OK {
		return driver.ErrBadConn
	}
	return nil
}

func (conn Conn) Begin() (driver.Tx, error) {
	sql := C.CString("begin;")
	defer C.free(unsafe.Pointer(sql))
	r := C.sqlite3_exec(conn.db, sql, nil, nil, nil)
	if r != C.SQLITE_OK {
		return nil, dbError(conn.db)
	}
	return &Tx{db: conn.db}, nil
}

type Stmt struct {
	stmt *C.sqlite3_stmt
}

func (stmt Stmt) Close() error {
	r := C.sqlite3_finalize(stmt.stmt)
	if r != C.SQLITE_OK {
		return driver.ErrBadConn
	}
	return nil
}

func (stmt Stmt) NumInput() int {
	return int(C.sqlite3_bind_parameter_count(stmt.stmt))
}

func (stmt Stmt) bind(args []driver.Value) error {
	if C.sqlite3_clear_bindings(stmt.stmt) != C.SQLITE_OK {
		return stmtError(stmt.stmt)
	}
	for i, arg := range args {
		r := C.int(-1)
		switch val := arg.(type) {
		case int64:
			r = C.sqlite3_bind_int64(stmt.stmt, C.int(i+1), C.sqlite3_int64(val))
		case float64:
			r = C.sqlite3_bind_double(stmt.stmt, C.int(i+1), C.double(val))
		case bool:
			if val {
				r = C.sqlite3_bind_int64(stmt.stmt, C.int(i+1), C.sqlite3_int64(1))
			} else {
				r = C.sqlite3_bind_int64(stmt.stmt, C.int(i+1), C.sqlite3_int64(0))
			}
		case nil:
			r = C.sqlite3_bind_null(stmt.stmt, C.int(i+1))
		case string:
			str := C.CString(val)
			defer C.free(unsafe.Pointer(str))
			l := C.int(C.strlen(str))
			r = C.bind_text(stmt.stmt, C.int(i+1), str, l)
		default:
			panic("unsupported type")
		}

		if r != C.SQLITE_OK {
			return stmtError(stmt.stmt)
		}
	}
	return nil
}

func (stmt Stmt) Exec(args []driver.Value) (driver.Result, error) {
	if err := stmt.bind(args); err != nil {
		return nil, err
	}

	r := C.sqlite3_step(stmt.stmt)
	db := C.sqlite3_db_handle(stmt.stmt)

	if r != C.SQLITE_DONE && r != C.SQLITE_ROW {
		return nil, dbError(db)
	}

	var result Result
	result.rowsAffected = int64(C.sqlite3_changes(db))
	result.lastInsertId = int64(C.sqlite3_last_insert_rowid(db))
	return &result, nil
}

func (stmt Stmt) Query(args []driver.Value) (driver.Rows, error) {
	if err := stmt.bind(args); err != nil {
		return nil, err
	}
	return &Rows{stmt: stmt.stmt}, nil
}

type Result struct {
	rowsAffected int64
	lastInsertId int64
}

func (result Result) LastInsertId() (int64, error) {
	return result.lastInsertId, nil
}

func (result Result) RowsAffected() (int64, error) {
	return result.rowsAffected, nil
}

type Rows struct {
	stmt *C.sqlite3_stmt
}

func (rows Rows) Columns() []string {
	count := int(C.sqlite3_column_count(rows.stmt))
	cols := make([]string, count)
	for i := 0; i < count; i++ {
		cols[i] = C.GoString(C.sqlite3_column_name(rows.stmt, C.int(i)))
	}
	return cols
}

func (rows Rows) Close() error {
	if C.sqlite3_reset(rows.stmt) != C.SQLITE_OK {
		return driver.ErrBadConn
	}
	return nil
}

func (rows Rows) Next(dest []driver.Value) error {
	r := C.sqlite3_step(rows.stmt)
	if r == C.SQLITE_DONE {
		return io.EOF
	}
	if r != C.SQLITE_ROW {
		return stmtError(rows.stmt)
	}

	count := len(dest)
	for i := 0; i < count; i++ {
		t := C.sqlite3_column_type(rows.stmt, C.int(i))
		switch t {
		case C.SQLITE_INTEGER:
			dest[i] = int64(C.sqlite3_column_int64(rows.stmt, C.int(i)))
		case C.SQLITE_FLOAT:
			dest[i] = float64(C.sqlite3_column_double(rows.stmt, C.int(i)))
		case C.SQLITE_NULL:
			dest[i] = nil
		case C.SQLITE_TEXT:
			n := C.sqlite3_column_bytes(rows.stmt, C.int(i))
			blob := C.sqlite3_column_blob(rows.stmt, C.int(i))
			dest[i] = C.GoBytes(blob, n)
		default:
			panic("unsupported type")
		}
	}
	return nil
}

type Tx struct {
	db *C.sqlite3
}

func (tx Tx) Commit() error {
	sql := C.CString("commit;")
	defer C.free(unsafe.Pointer(sql))
	r := C.sqlite3_exec(tx.db, sql, nil, nil, nil)
	if r != C.SQLITE_OK {
		return dbError(tx.db)
	}
	return nil
}

func (tx Tx) Rollback() error {
	sql := C.CString("rollback;")
	defer C.free(unsafe.Pointer(sql))
	r := C.sqlite3_exec(tx.db, sql, nil, nil, nil)
	if r != C.SQLITE_OK {
		return dbError(tx.db)
	}
	return nil
}

func dbError(db *C.sqlite3) error {
	return errors.New(C.GoString(C.sqlite3_errmsg(db)))
}

func stmtError(stmt *C.sqlite3_stmt) error {
	return dbError(C.sqlite3_db_handle(stmt))
}
