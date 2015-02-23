gomdb
=====

Go wrapper for OpenLDAP Lightning Memory-Mapped Database (LMDB).
Read more about LMDB here: http://symas.com/mdb/

GoDoc available here: http://godoc.org/github.com/msackman/gomdb

Build
=======

`go get github.com/msackman/gomdb`

You need to ensure you have installed lmdb yourself.

TODO
======

 * write more documentation
 * write more unit test
 * benchmark
 * figure out how can you write go binding for `MDB_comp_func` and `MDB_rel_func`
 * Handle go `*Cursor` close with `txn.Commit` and `txn.Abort` transparently

