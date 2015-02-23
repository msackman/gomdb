package mdb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

type MyDatabases struct {
	One *DBISettings
	Two *DBISettings
}

func TestServerPutGet(t *testing.T) {
	mydb := &MyDatabases{
		One: &DBISettings{Flags: CREATE},
	}

	withDatabases(t, mydb, func(server *MDBServer) {
		key := "mykey"
		val := "myval"
		result := 42
		future := server.ReadWriteTransaction(func(rwtxn *RWTxn) (interface{}, error) {
			return result, rwtxn.Put(mydb.One, []byte(key), []byte(val), 0)
		})
		future.Force()
		expectFutureErrorFree(t, future, "Unable to put value:")
		if future.Result.(int) != result {
			t.Fatal("Unexpected result received:", future.Result)
		}

		getAndCheckValue(t, server, mydb.One, []byte(key), []byte(val))
	})
}

func TestServerDatabasesDistict(t *testing.T) {
	mydb := &MyDatabases{
		One: &DBISettings{Flags: CREATE},
		Two: &DBISettings{Flags: CREATE},
	}

	withDatabases(t, mydb, func(server *MDBServer) {
		key := "mykey"
		val := "myval"
		otherval := "myotherval"
		future := server.ReadWriteTransaction(func(rwtxn *RWTxn) (interface{}, error) {
			err := rwtxn.Put(mydb.One, []byte(key), []byte(val), 0)
			if err != nil {
				return nil, err
			}
			err = rwtxn.Put(mydb.Two, []byte(key), []byte(otherval), 0)
			return nil, err
		})
		future.Force()
		expectFutureErrorFree(t, future, "Unable to put values:")

		getAndCheckValue(t, server, mydb.One, []byte(key), []byte(val))
		getAndCheckValue(t, server, mydb.Two, []byte(key), []byte(otherval))
	})
}

func TestServerGetMissingDoesntKill(t *testing.T) {
	mydb := &MyDatabases{
		One: &DBISettings{Flags: CREATE},
	}
	withDatabases(t, mydb, func(server *MDBServer) {
		key := "mykey"
		missingKey := "myotherkey"
		val := "myval"
		future := server.ReadWriteTransaction(func(rwtxn *RWTxn) (interface{}, error) {
			return nil, rwtxn.Put(mydb.One, []byte(key), []byte(val), 0)
		})
		future.Force()
		expectFutureErrorFree(t, future, "Unable to put value:")

		future = server.ReadonlyTransaction(func(rtxn *RTxn) (interface{}, error) {
			return rtxn.Get(mydb.One, []byte(missingKey))
		})
		future.Force()
		if future.Error != NotFound {
			t.Fatal("Was expecting NotFound error. Got:", future.Error)
		}

		future = server.ReadonlyTransaction(func(rtxn *RTxn) (interface{}, error) {
			return rtxn.Get(mydb.One, []byte(key))
		})
		future.Force()
		expectFutureErrorFree(t, future, "Unable to get value:")
		if val != string(future.Result.([]byte)) {
			t.Fatal("Unexpected result of get:", future.Result)
		}
	})
}

func getAndCheckValue(t *testing.T, server *MDBServer, db *DBISettings, key, val []byte) *TransactionFuture {
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
	future.Force()
	expectFutureErrorFree(t, future, "Unable to get value:")
	return future
}

func expectFutureErrorFree(t *testing.T, future *TransactionFuture, msg string) {
	future.Force() // it's idempotent anyway, so let's just be safe
	if future.Error != nil {
		t.Fatal(msg, future.Error)
	}
}

func withDatabases(t *testing.T, i interface{}, testFun func(*MDBServer)) {
	path, err := ioutil.TempDir("/tmp", "mdb_test")
	if err != nil {
		t.Fatalf("Cannot create temporary directory")
	}
	err = os.MkdirAll(path, 0770)
	if err != nil {
		t.Fatalf("Cannot create directory: %s", path)
	}
	defer os.RemoveAll(path)

	server, err := NewMDBServer(path, WRITEMAP, 0600, 10485760, 1, i)
	if err != nil {
		t.Fatalf("Cannot start server: %v", err)
	}
	defer server.Shutdown()

	testFun(server)
}
