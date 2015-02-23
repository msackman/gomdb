package mdb

/*
#include <lmdb.h>
*/
import "C"

import (
	"errors"
)

// MDB_cursor_op
const (
	FIRST = iota
	FIRST_DUP
	GET_BOTH
	GET_RANGE
	GET_CURRENT
	GET_MULTIPLE
	LAST
	LAST_DUP
	NEXT
	NEXT_DUP
	NEXT_MULTIPLE
	NEXT_NODUP
	PREV
	PREV_DUP
	PREV_NODUP
	SET
	SET_KEY
	SET_RANGE
)

func (cursor *Cursor) Close() error {
	if cursor.cursor == nil {
		return errors.New("Cursor already closed")
	}
	C.mdb_cursor_close(cursor.cursor)
	cursor.cursor = nil
	return nil
}

func (cursor *Cursor) Txn() *Txn {
	var txn *C.MDB_txn
	txn = C.mdb_cursor_txn(cursor.cursor)
	if txn != nil {
		return &Txn{txn}
	}
	return nil
}

func (cursor *Cursor) DBI() DBI {
	var dbi C.MDB_dbi
	dbi = C.mdb_cursor_dbi(cursor.cursor)
	return DBI(dbi)
}

// Retrieves the low-level MDB cursor.
func (cursor *Cursor) MdbCursor() *C.MDB_cursor {
	return cursor.cursor
}

func (cursor *Cursor) Get(set_key, sval []byte, op uint) (key, val []byte, err error) {
	k, v, err := cursor.GetVal(set_key, sval, op)
	if err != nil {
		return nil, nil, err
	}
	return k.Bytes(), v.Bytes(), nil
}

func (cursor *Cursor) GetVal(key, val []byte, op uint) (Val, Val, error) {
	ckey := Wrap(key)
	cval := Wrap(val)
	ret := C.mdb_cursor_get(cursor.cursor, (*C.MDB_val)(&ckey), (*C.MDB_val)(&cval), C.MDB_cursor_op(op))
	return ckey, cval, errno(ret)
}

func (cursor *Cursor) Put(key, val []byte, flags uint) error {
	ckey := Wrap(key)
	cval := Wrap(val)
	ret := C.mdb_cursor_put(cursor.cursor, (*C.MDB_val)(&ckey), (*C.MDB_val)(&cval), C.uint(flags))
	return errno(ret)
}

func (cursor *Cursor) Del(flags uint) error {
	ret := C.mdb_cursor_del(cursor.cursor, C.uint(flags))
	return errno(ret)
}

func (cursor *Cursor) Count() (uint64, error) {
	var size C.size_t
	ret := C.mdb_cursor_count(cursor.cursor, &size)
	if ret != SUCCESS {
		return 0, errno(ret)
	}
	return uint64(size), nil
}
