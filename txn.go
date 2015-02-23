package mdb

/*
#include <stdlib.h>
#include <lmdb.h>
*/
import "C"

import (
	"math"
	"unsafe"
)

// DBIOpen Database Flags
const (
	REVERSEKEY = C.MDB_REVERSEKEY // use reverse string keys
	DUPSORT    = C.MDB_DUPSORT    // use sorted duplicates
	INTEGERKEY = C.MDB_INTEGERKEY // numeric keys in native byte order. The keys must all be of the same size.
	DUPFIXED   = C.MDB_DUPFIXED   // with DUPSORT, sorted dup items have fixed size
	INTEGERDUP = C.MDB_INTEGERDUP // with DUPSORT, dups are numeric in native byte order
	REVERSEDUP = C.MDB_REVERSEDUP // with DUPSORT, use reverse string dups
	CREATE     = C.MDB_CREATE     // create DB if not already existing
)

// put flags
const (
	NODUPDATA   = C.MDB_NODUPDATA
	NOOVERWRITE = C.MDB_NOOVERWRITE
	RESERVE     = C.MDB_RESERVE
	APPEND      = C.MDB_APPEND
	APPENDDUP   = C.MDB_APPENDDUP
	CURRENT     = C.MDB_CURRENT
)

// Txn is Opaque structure for a transaction handle.
// All database operations require a transaction handle.
// Transactions may be read-only or read-write.
type Txn struct {
	txn *C.MDB_txn
}

func (env *Env) BeginTxn(parent *Txn, flags uint) (*Txn, error) {
	var txn *C.MDB_txn
	var ptxn *C.MDB_txn
	if parent == nil {
		ptxn = nil
	} else {
		ptxn = parent.txn
	}
	ret := C.mdb_txn_begin(env.env, ptxn, C.uint(flags), &txn)
	if ret != SUCCESS {
		return nil, errno(ret)
	}
	return &Txn{txn}, nil
}

func (txn *Txn) Commit() error {
	ret := C.mdb_txn_commit(txn.txn)
	// The transaction handle is freed if there was no error
	if ret == SUCCESS {
		txn.txn = nil
	}
	return errno(ret)
}

func (txn *Txn) Abort() {
	if txn.txn == nil {
		return
	}
	C.mdb_txn_abort(txn.txn)
	// The transaction handle is always freed.
	txn.txn = nil
}

func (txn *Txn) Reset() {
	C.mdb_txn_reset(txn.txn)
}

func (txn *Txn) Renew() error {
	ret := C.mdb_txn_renew(txn.txn)
	return errno(ret)
}

func (txn *Txn) DBIOpen(name *string, flags uint) (DBI, error) {
	var dbi C.MDB_dbi
	var cname *C.char
	if name == nil {
		cname = nil
	} else {
		cname = C.CString(*name)
		defer C.free(unsafe.Pointer(cname))
	}
	ret := C.mdb_dbi_open(txn.txn, cname, C.uint(flags), &dbi)
	if ret != SUCCESS {
		return DBI(math.NaN()), errno(ret)
	}
	return DBI(dbi), nil
}

func (txn *Txn) Stat(dbi DBI) (*Stat, error) {
	var c_stat C.MDB_stat
	ret := C.mdb_stat(txn.txn, C.MDB_dbi(dbi), &c_stat)
	if ret != SUCCESS {
		return nil, errno(ret)
	}
	stat := Stat{PSize: uint(c_stat.ms_psize),
		Depth:         uint(c_stat.ms_depth),
		BranchPages:   uint64(c_stat.ms_branch_pages),
		LeafPages:     uint64(c_stat.ms_leaf_pages),
		OverflowPages: uint64(c_stat.ms_overflow_pages),
		Entries:       uint64(c_stat.ms_entries)}
	return &stat, nil
}

func (txn *Txn) Drop(dbi DBI, del int) error {
	ret := C.mdb_drop(txn.txn, C.MDB_dbi(dbi), C.int(del))
	return errno(ret)
}

func (txn *Txn) Get(dbi DBI, key []byte) ([]byte, error) {
	val, err := txn.GetVal(dbi, key)
	if err != nil {
		return nil, err
	}
	return val.Bytes(), nil
}

func (txn *Txn) GetVal(dbi DBI, key []byte) (Val, error) {
	ckey := Wrap(key)
	var cval Val
	ret := C.mdb_get(txn.txn, C.MDB_dbi(dbi), (*C.MDB_val)(&ckey), (*C.MDB_val)(&cval))
	return cval, errno(ret)
}

func (txn *Txn) Put(dbi DBI, key []byte, val []byte, flags uint) error {
	ckey := Wrap(key)
	cval := Wrap(val)
	ret := C.mdb_put(txn.txn, C.MDB_dbi(dbi), (*C.MDB_val)(&ckey), (*C.MDB_val)(&cval), C.uint(flags))
	return errno(ret)
}

func (txn *Txn) Del(dbi DBI, key, val []byte) error {
	ckey := Wrap(key)
	if val == nil {
		ret := C.mdb_del(txn.txn, C.MDB_dbi(dbi), (*C.MDB_val)(&ckey), nil)
		return errno(ret)
	}
	cval := Wrap(val)
	ret := C.mdb_del(txn.txn, C.MDB_dbi(dbi), (*C.MDB_val)(&ckey), (*C.MDB_val)(&cval))
	return errno(ret)
}

type Cursor struct {
	cursor *C.MDB_cursor
}

func (txn *Txn) CursorOpen(dbi DBI) (*Cursor, error) {
	var cursor *C.MDB_cursor
	ret := C.mdb_cursor_open(txn.txn, C.MDB_dbi(dbi), &cursor)
	if ret != SUCCESS {
		return nil, errno(ret)
	}
	return &Cursor{cursor}, nil
}

func (txn *Txn) CursorRenew(cursor *Cursor) error {
	ret := C.mdb_cursor_renew(txn.txn, cursor.cursor)
	return errno(ret)
}
