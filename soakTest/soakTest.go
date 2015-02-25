package main

import (
	"flag"
	"fmt"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

type DBs struct {
	Test *mdbs.DBISettings
}

const (
	keySize   = 16
	valSize   = 96
	terabyte  = 1099511627776
	openFlags = mdb.WRITEMAP //| mdb.MAPASYNC // try |mdb.MAPASYNC for ludicrous speed
)

func main() {
	log.SetPrefix("MDB Soak Test ")
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Println(os.Args)

	procs := runtime.NumCPU()
	if procs < 2 {
		procs = 2
	}
	runtime.GOMAXPROCS(procs)

	var records, readers int
	var rewriter bool

	flag.IntVar(&records, "records", 1000, "Number of records to write to database (default 1000)")
	flag.IntVar(&readers, "readers", 1, "Number of concurrent readers to use (default 1)")
	flag.BoolVar(&rewriter, "rewriter", false, "Run a rewriter concurrently (default false)")
	flag.Parse()

	if records < 1 || readers < 0 {
		log.Fatal("records must be > 0 and readers must be >= 0")
	}

	dir, err := ioutil.TempDir("", "mdb_soak_test")
	if err != nil {
		log.Fatal("Cannot create temporary directory")
	}
	defer os.RemoveAll(dir)
	err = os.MkdirAll(dir, 0770)
	if err != nil {
		log.Fatal("Cannot create directory:", dir)
	}
	log.Println("Using dir", dir)

	dbs := &DBs{
		Test: &mdbs.DBISettings{Flags: mdb.CREATE | mdb.INTEGERKEY},
	}
	server, err := mdbs.NewMDBServer(dir, openFlags, 0600, terabyte, 0, dbs)
	if err != nil {
		log.Fatal("Cannot start server:", err)
	}
	defer server.Shutdown()

	popStart := time.Now()
	if err = populate(records, server, dbs); err != nil {
		log.Fatal(err)
	}
	popEnd := time.Now()
	popTime := popEnd.Sub(popStart)
	popRate := float64(int64(records)*time.Second.Nanoseconds()) / float64(popTime.Nanoseconds())
	log.Println("Populating DB with", records, "records took", popTime, "(", popRate, "records/sec )")

	for idx := 0; idx < readers; idx++ {
		go worker(int64(records), server, dbs, readers, idx, false)
	}

	if rewriter {
		go worker(int64(records), server, dbs, 1, -1, true)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, os.Interrupt)
	<-sigs
}

func populate(records int, server *mdbs.MDBServer, dbs *DBs) error {
	key := make([]byte, keySize)
	val := make([]byte, valSize)
	return server.ReadWriteTransaction(false, func(txn *mdbs.RWTxn) (interface{}, error) {
		for idx := 0; idx < records; idx++ {
			int64ToBytes(int64(idx), key)
			int64ToBytes(int64(idx), val)
			if err := txn.Put(dbs.Test, key, val, 0); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}).Error()
}

func worker(records int64, server *mdbs.MDBServer, dbs *DBs, readers, id int, write bool) error {
	msg := fmt.Sprint(id, ": Read")
	if write {
		msg = ": Wrote"
	}
	start := time.Now()
	count := 0
	ticker := time.NewTicker(time.Duration(readers) * time.Second)
	var err error
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			elapsed := now.Sub(start)
			rate := float64(int64(count)*time.Second.Nanoseconds()) / float64(elapsed.Nanoseconds())
			log.Println(msg, count, "records in", elapsed, "(", rate, "records/sec )")
			start = now
			count = 0
		default:
			if write {
				err = server.ReadWriteTransaction(true, func(txn *mdbs.RWTxn) (interface{}, error) {
					keyNum := rand.Int63n(records)
					key := make([]byte, keySize)
					int64ToBytes(keyNum, key)
					val, err1 := txn.Get(dbs.Test, key)
					if err1 != nil {
						return nil, err1
					}
					num := bytesToInt64(val)
					if num < keyNum {
						return nil, fmt.Errorf("Expected val (%v) >= key (%v)", num, keyNum)
					}
					int64ToBytes(num+1, val)
					return nil, txn.Put(dbs.Test, key, val, 0)
				}).Error()
			} else {
				err = server.ReadonlyTransaction(func(txn *mdbs.RTxn) (interface{}, error) {
					keyNum := rand.Int63n(records)
					key := make([]byte, keySize)
					int64ToBytes(keyNum, key)
					val, err1 := txn.GetVal(dbs.Test, key)
					if err1 != nil {
						return nil, err1
					}
					num := bytesToInt64(val.BytesNoCopy())
					if num < keyNum {
						return nil, fmt.Errorf("Expected val (%v) >= key (%v)", num, keyNum)
					}
					return nil, nil
				}).Error()
			}
			if err != nil {
				log.Fatal(err)
			}
			count++
		}
	}
}

func int64ToBytes(n int64, ary []byte) {
	for idx := 0; idx < 8; idx++ {
		shift := uint(idx * 8)
		ary[idx] = byte((n >> shift) & 0xFF)
	}
}

func bytesToInt64(ary []byte) int64 {
	num := int64(0)
	for idx := 0; idx < 8; idx++ {
		shift := uint(idx * 8)
		num = num + (int64(ary[idx]) << shift)
	}
	return num
}
