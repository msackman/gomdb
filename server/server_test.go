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
	One *DBISettings
	Two *DBISettings
}

func TestServerPutGet(t *testing.T) {
	mydb := &MyDatabases{
		One: &DBISettings{Flags: mdb.CREATE},
	}

	withMDBServer(t, mydb, func(server *MDBServer) {
		key := "mykey"
		val := "myval"
		result := 42
		future := server.ReadWriteTransaction(false, func(rwtxn *RWTxn) (interface{}, error) {
			return result, rwtxn.Put(mydb.One, []byte(key), []byte(val), 0)
		})
		expectFutureErrorFree(t, future, "Unable to put value:")
		if res, _ := future.ResultError(); res.(int) != result {
			t.Fatal("Unexpected result received:", res)
		}

		getAndCheckValue(t, server, mydb.One, []byte(key), []byte(val))
	})
}

func TestServerDatabasesDistict(t *testing.T) {
	mydb := &MyDatabases{
		One: &DBISettings{Flags: mdb.CREATE},
		Two: &DBISettings{Flags: mdb.CREATE},
	}

	withMDBServer(t, mydb, func(server *MDBServer) {
		key := "mykey"
		val := "myval"
		otherval := "myotherval"
		future := server.ReadWriteTransaction(false, func(rwtxn *RWTxn) (interface{}, error) {
			err := rwtxn.Put(mydb.One, []byte(key), []byte(val), 0)
			if err != nil {
				return nil, err
			}
			err = rwtxn.Put(mydb.Two, []byte(key), []byte(otherval), 0)
			return nil, err
		})
		expectFutureErrorFree(t, future, "Unable to put values:")

		getAndCheckValue(t, server, mydb.One, []byte(key), []byte(val))
		getAndCheckValue(t, server, mydb.Two, []byte(key), []byte(otherval))
	})
}

func TestServerGetMissingDoesntKill(t *testing.T) {
	mydb := &MyDatabases{
		One: &DBISettings{Flags: mdb.CREATE},
	}
	withMDBServer(t, mydb, func(server *MDBServer) {
		key := "mykey"
		missingKey := "myotherkey"
		val := "myval"
		future := server.ReadWriteTransaction(false, func(rwtxn *RWTxn) (interface{}, error) {
			return nil, rwtxn.Put(mydb.One, []byte(key), []byte(val), 0)
		})
		expectFutureErrorFree(t, future, "Unable to put value:")

		future = server.ReadonlyTransaction(func(rtxn *RTxn) (interface{}, error) {
			return rtxn.Get(mydb.One, []byte(missingKey))
		})
		if _, err := future.ResultError(); err != mdb.NotFound {
			t.Fatal("Was expecting NotFound error. Got:", err)
		}

		future = server.ReadonlyTransaction(func(rtxn *RTxn) (interface{}, error) {
			return rtxn.Get(mydb.One, []byte(key))
		})
		expectFutureErrorFree(t, future, "Unable to get value:")
		if res, _ := future.ResultError(); val != string(res.([]byte)) {
			t.Fatal("Unexpected result of get:", res)
		}
	})
}

func getAndCheckValue(t *testing.T, server *MDBServer, db *DBISettings, key, val []byte) TransactionFuture {
	future := server.ReadonlyTransaction(func(rtxn *RTxn) (interface{}, error) {
		v, err := rtxn.Get(db, []byte(key))
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(v, val) {
			return nil, fmt.Errorf("Unexpected result of get: %v", v)
		}
		return nil, nil
	})
	expectFutureErrorFree(t, future, "Unable to get value:")
	return future
}

func expectFutureErrorFree(t *testing.T, future TransactionFuture, msg string) {
	if _, err := future.ResultError(); err != nil {
		t.Fatal(msg, err)
	}
}

func withMDBServer(t *testing.T, i interface{}, testFun func(*MDBServer)) {
	path, err := ioutil.TempDir("/tmp", "mdb_test")
	if err != nil {
		t.Fatalf("Cannot create temporary directory")
	}
	err = os.MkdirAll(path, 0770)
	if err != nil {
		t.Fatalf("Cannot create directory: %s", path)
	}
	defer os.RemoveAll(path)

	server, err := NewMDBServer(path, mdb.WRITEMAP, 0600, 10485760, 1, time.Millisecond, i)
	if err != nil {
		t.Fatalf("Cannot start server: %v", err)
	}
	defer server.Shutdown()

	testFun(server)
}
