package main

import (
	"flag"
	"fmt"
	"github.com/go-kit/kit/log"
	mdb "github.com/msackman/gomdb"
	mdbs "github.com/msackman/gomdb/server"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

type DBs struct {
	*mdbs.MDBServer
	Test *mdbs.DBISettings
}

func (dbs *DBs) Clone() mdbs.DBIsInterface {
	return &DBs{Test: dbs.Test.Clone()}
}

func (dbs *DBs) SetServer(server *mdbs.MDBServer) {
	dbs.MDBServer = server
}

const (
	keySize   = 16
	valSize   = 96
	mapSize   = 10485760
	openFlags = 0 // | mdb.NOSYNC // try | mdb.NOSYNC for ludicrous speed
)

func main() {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	logger.Log("args", fmt.Sprint(os.Args))

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
		logger.Log("error", "records must be > 0 and readers must be >= 0")
		os.Exit(1)
	}

	dir, err := ioutil.TempDir("", "mdb_soak_test")
	if err != nil {
		logger.Log("error", fmt.Sprintf("Cannot create temporary directory: %v", err))
		os.Exit(1)
	}
	defer os.RemoveAll(dir)
	err = os.MkdirAll(dir, 0770)
	if err != nil {
		logger.Log("error", fmt.Sprintf("Cannot create directory: %v", err), "dir", dir)
	}
	logger.Log("dir", dir)

	dbs := &DBs{
		Test: &mdbs.DBISettings{Flags: mdb.CREATE | mdb.INTEGERKEY},
	}
	server, err := mdbs.NewMDBServer(dir, openFlags, 0600, mapSize, time.Millisecond, dbs, logger)
	if err != nil {
		logger.Log("error", err, "msg", "Cannot start server.")
	}
	dbs = server.(*DBs)
	defer dbs.Shutdown()

	popStart := time.Now()
	if err = populate(records, dbs); err != nil {
		logger.Log("error", err)
		os.Exit(1)
	}
	popEnd := time.Now()
	popTime := popEnd.Sub(popStart)
	popRate := float64(int64(records)*time.Second.Nanoseconds()) / float64(popTime.Nanoseconds())
	logger.Log("msg", fmt.Sprintf("Populating DB with %d records took %v (%v records/sec)",
		records, popTime, popRate))

	for idx := 0; idx < readers; idx++ {
		go worker(logger, int64(records), dbs, readers, idx, false)
	}

	if rewriter {
		go worker(logger, int64(records), dbs, 1, 0, true)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, os.Interrupt)
	<-sigs
}

func populate(records int, dbs *DBs) error {
	key := make([]byte, keySize)
	val := make([]byte, valSize)
	_, err := dbs.ReadWriteTransaction(func(txn *mdbs.RWTxn) interface{} {
		for idx := 0; idx < records; idx++ {
			int64ToBytes(int64(idx), key)
			int64ToBytes(int64(idx), val)
			if err := txn.Put(dbs.Test, key, val, 0); err != nil {
				return nil
			}
		}
		return nil
	}).ResultError()
	return err
}

func worker(logger log.Logger, records int64, dbs *DBs, readers, id int, write bool) error {
	if write {
		logger = log.With(logger, "writer", id)
	} else {
		logger = log.With(logger, "reader", id)
	}
	start := time.Now()
	count := 0
	ticker := time.NewTicker(time.Duration(readers) * time.Second)
	randSource := rand.New(rand.NewSource(time.Now().UnixNano()))
	var err error
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			elapsed := now.Sub(start)
			rate := float64(int64(count)*time.Second.Nanoseconds()) / float64(elapsed.Nanoseconds())
			logger.Log("msg", fmt.Sprintf("%d records, in %v (%v records/sec)", count, elapsed, rate))
			start = now
			count = 0
		default:
			if write {
				keyNum := randSource.Int63n(records)
				key := make([]byte, keySize)
				int64ToBytes(keyNum, key)
				_, err = dbs.ReadWriteTransaction(func(txn *mdbs.RWTxn) interface{} {
					val, err1 := txn.Get(dbs.Test, key)
					if err1 != nil {
						return nil
					}
					num := bytesToInt64(val)
					if num < keyNum {
						panic(fmt.Errorf("Expected val (%v) >= key (%v)", num, keyNum))
					}
					int64ToBytes(num+1, val)
					txn.Put(dbs.Test, key, val, 0)
					return nil
				}).ResultError()
			} else {
				keyNum := randSource.Int63n(records)
				key := make([]byte, keySize)
				int64ToBytes(keyNum, key)
				_, err = dbs.ReadonlyTransaction(func(txn *mdbs.RTxn) interface{} {
					val, err1 := txn.GetVal(dbs.Test, key)
					if err1 != nil {
						return nil
					}
					num := bytesToInt64(val.BytesNoCopy())
					if num < keyNum {
						panic(fmt.Errorf("Expected val (%v) >= key (%v)", num, keyNum))
					}
					return nil
				}).ResultError()
			}
			if err != nil {
				logger.Log("error", err)
				os.Exit(1)
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
