package server

import (
	"bytes"
	"fmt"
	mdb "github.com/msackman/gomdb"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

type MyDatabases struct {
	*MDBServer
	One *DBISettings
	Two *DBISettings
}

func (md *MyDatabases) Clone() DBIsInterface {
	return &MyDatabases{
		One: md.One.Clone(),
		Two: md.Two.Clone(),
	}
}

func (md *MyDatabases) SetServer(server *MDBServer) {
	md.MDBServer = server
}

func TestServerPutGet(t *testing.T) {
	mydb := &MyDatabases{
		One: &DBISettings{Flags: mdb.CREATE},
	}

	withMDBServer(t, mydb, func(mydb *MyDatabases) {
		key := "mykey"
		val := "myval"
		result := 42
		future := mydb.ReadWriteTransaction(false, func(rwtxn *RWTxn) interface{} {
			if rwtxn.Put(mydb.One, []byte(key), []byte(val), 0) != nil {
				return nil
			}
			return result
		})
		expectFutureErrorFree(t, future, "Unable to put value:")
		if res, _ := future.ResultError(); res.(int) != result {
			t.Fatal("Unexpected result received:", res)
		}

		getAndCheckValue(t, mydb, mydb.One, []byte(key), []byte(val))
	})
}

func TestServerDatabasesDistict(t *testing.T) {
	mydb := &MyDatabases{
		One: &DBISettings{Flags: mdb.CREATE},
		Two: &DBISettings{Flags: mdb.CREATE},
	}

	withMDBServer(t, mydb, func(mydb *MyDatabases) {
		key := "mykey"
		val := "myval"
		otherval := "myotherval"
		future := mydb.ReadWriteTransaction(false, func(rwtxn *RWTxn) interface{} {
			if rwtxn.Put(mydb.One, []byte(key), []byte(val), 0) != nil {
				return nil
			}
			rwtxn.Put(mydb.Two, []byte(key), []byte(otherval), 0)
			return nil
		})
		expectFutureErrorFree(t, future, "Unable to put values:")

		getAndCheckValue(t, mydb, mydb.One, []byte(key), []byte(val))
		getAndCheckValue(t, mydb, mydb.Two, []byte(key), []byte(otherval))
	})
}

func TestServerGetMissingDoesntKill(t *testing.T) {
	mydb := &MyDatabases{
		One: &DBISettings{Flags: mdb.CREATE},
	}
	withMDBServer(t, mydb, func(mydb *MyDatabases) {
		key := "mykey"
		missingKey := "myotherkey"
		val := "myval"
		future := mydb.ReadWriteTransaction(false, func(rwtxn *RWTxn) interface{} {
			rwtxn.Put(mydb.One, []byte(key), []byte(val), 0)
			return nil
		})
		expectFutureErrorFree(t, future, "Unable to put value:")

		future = mydb.ReadonlyTransaction(func(rtxn *RTxn) interface{} {
			_, err := rtxn.Get(mydb.One, []byte(missingKey))
			return err
		})
		if res, _ := future.ResultError(); res != mdb.NotFound {
			t.Fatal("Was expecting NotFound result. Got:", res)
		}

		future = mydb.ReadonlyTransaction(func(rtxn *RTxn) interface{} {
			if bites, err := rtxn.Get(mydb.One, []byte(key)); err == nil {
				return bites
			} else {
				rtxn.Error(err)
				return nil
			}
		})
		expectFutureErrorFree(t, future, "Unable to get value:")
		if res, _ := future.ResultError(); val != string(res.([]byte)) {
			t.Fatal("Unexpected result of get:", res)
		}
	})
}

func getAndCheckValue(t *testing.T, server *MyDatabases, db *DBISettings, key, val []byte) TransactionFuture {
	future := server.ReadonlyTransaction(func(rtxn *RTxn) interface{} {
		v, err := rtxn.Get(db, []byte(key))
		if err == nil {
			if !bytes.Equal(v, val) {
				rtxn.Error(fmt.Errorf("Unexpected result of get: %v", v))
			}
		} else {
			rtxn.Error(err)
		}
		return nil
	})
	expectFutureErrorFree(t, future, "Unable to get value:")
	return future
}

func expectFutureErrorFree(t *testing.T, future TransactionFuture, msg string) {
	if _, err := future.ResultError(); err != nil {
		t.Fatal(msg, err)
	}
}

func withMDBServer(t *testing.T, i *MyDatabases, testFun func(*MyDatabases)) {
	path, err := ioutil.TempDir("/tmp", "mdb_test")
	if err != nil {
		t.Fatalf("Cannot create temporary directory")
	}
	err = os.MkdirAll(path, 0770)
	if err != nil {
		t.Fatalf("Cannot create directory: %s", path)
	}
	defer os.RemoveAll(path)

	iface, err := NewMDBServer(path, mdb.WRITEMAP, 0600, 10485760, 1, time.Millisecond, i)
	if err != nil {
		t.Fatalf("Cannot start server: %v", err)
	}
	i = iface.(*MyDatabases)
	defer i.Shutdown()

	testFun(i)
}
