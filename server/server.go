package server

import (
	"errors"
	mdb "github.com/msackman/gomdb"
	"log"
	"reflect"
	"runtime"
)

type ReadonlyTransaction func(rtxn *RTxn) (interface{}, error)
type ReadWriteTransaction func(rwtxn *RWTxn) (interface{}, error)

type DBISettings struct {
	Flags uint
	dbi   mdb.DBI
}

type MDBServer struct {
	writerChan chan *mdbQuery
	readerChan chan *mdbQuery
	readers    []*mdbReader
	env        *mdb.Env
	rwtxn      *RWTxn
}

type mdbReader struct {
	queryChan chan *mdbQuery
	server    *MDBServer
	rtxn      *RTxn
}

type mdbQuery struct {
	code       int
	payload    interface{}
	resultChan chan<- interface{}
}

const (
	queryShutdown = iota
	queryRunTxn   = iota
)

const (
	defaultChanSize = 64
)

var NotAStructPointer = errors.New("Not a pointer to a struct")
var UnexpectedMessage = errors.New("Unexpected message")

func NewMDBServer(path string, openFlags, filemode uint, mapSize uint64, numReaders int, dbiStruct interface{}) (*MDBServer, error) {
	writerChan := make(chan *mdbQuery, defaultChanSize)
	readerChan := make(chan *mdbQuery, defaultChanSize)
	if numReaders < 1 {
		numReaders = runtime.GOMAXPROCS(0) / 2 // with 0, just returns current value
		if numReaders < 1 {
			numReaders = 1
		}
	}
	server := &MDBServer{
		writerChan: writerChan,
		readerChan: readerChan,
		readers:    make([]*mdbReader, numReaders),
		rwtxn:      &RWTxn{},
	}
	resultChan := make(chan error, 0)
	go server.actorLoop(path, openFlags, filemode, mapSize, dbiStruct, resultChan)
	result := <-resultChan
	if result == nil {
		return server, nil
	} else {
		return nil, result
	}
}

func (mdb *MDBServer) ReadonlyTransaction(txn ReadonlyTransaction) *TransactionFuture {
	return mdb.submitTransaction(txn, mdb.readerChan)
}

func (mdb *MDBServer) ReadWriteTransaction(txn ReadWriteTransaction) *TransactionFuture {
	return mdb.submitTransaction(txn, mdb.writerChan)
}

func (mdb *MDBServer) Shutdown() {
	mdb.writerChan <- &mdbQuery{
		code: queryShutdown,
	}
}

func (mdb *MDBServer) submitTransaction(txn interface{}, queryChan chan<- *mdbQuery) *TransactionFuture {
	c := make(chan interface{}, 2)
	q := &mdbQuery{
		code:       queryRunTxn,
		payload:    txn,
		resultChan: c,
	}
	queryChan <- q
	return &TransactionFuture{resultChan: c}
}

func (server *MDBServer) actorLoop(path string, flags, mode uint, mapSize uint64, dbiStruct interface{}, initResult chan<- error) {
	runtime.LockOSThread()
	defer func() {
		if server.env != nil {
			server.env.Close()
		}
	}()
	if err := server.init(path, flags, mode, mapSize, dbiStruct); err != nil {
		initResult <- err
		return
	}
	initResult <- nil

	var err error
	terminate := false
	for !terminate {
		query := <-server.writerChan
		switch query.code {
		case queryShutdown:
			c := make(chan interface{}, 0)
			shutdown := &mdbQuery{
				code:       queryShutdown,
				resultChan: c,
			}
			for _, reader := range server.readers {
				reader.queryChan <- shutdown
				<-c // await death
			}
			terminate = true
		case queryRunTxn:
			err = server.handleRunTxn(query.payload.(ReadWriteTransaction), query.resultChan)
		default:
			err = UnexpectedMessage
		}
		terminate = terminate || err != nil
	}
	if err != nil {
		log.Println(err)
	}
}

func (server *MDBServer) init(path string, flags, mode uint, mapSize uint64, dbiStruct interface{}) error {
	env, err := mdb.NewEnv()
	if err != nil {
		return err
	}

	if err = env.SetMapSize(mapSize); err != nil {
		return err
	}
	if err = env.SetMaxReaders(uint(1 + len(server.readers))); err != nil {
		return err
	}

	dbiMap, err := analyzeDbiStruct(dbiStruct)
	if err != nil {
		return err
	}

	if l := len(dbiMap); l != 0 {
		if err = env.SetMaxDBs(mdb.DBI(l)); err != nil {
			return err
		}
	}
	if err = env.Open(path, flags, mode); err != nil {
		return err
	}
	server.env = env

	if l := len(dbiMap); l != 0 {
		txn, err := env.BeginTxn(nil, 0)
		if err != nil {
			return err
		}
		for name, value := range dbiMap {
			dbi, err := txn.DBIOpen(&name, value.Flags)
			if err != nil {
				txn.Abort()
				return err
			}
			value.dbi = dbi
		}
		if err = txn.Commit(); err != nil {
			return err
		}
	}

	for idx := range server.readers {
		reader := &mdbReader{
			queryChan: make(chan *mdbQuery, defaultChanSize),
			server:    server,
			rtxn:      &RTxn{},
		}
		server.readers[idx] = reader
		go reader.actorLoop()
	}
	return nil
}

func (server *MDBServer) handleRunTxn(txnFunc ReadWriteTransaction, resultChan chan<- interface{}) error {
	txn, err := server.env.BeginTxn(nil, 0)
	if err != nil {
		resultChan <- nil
		resultChan <- err
		return err
	}
	rwtxn := server.rwtxn
	rwtxn.txn = txn
	result, txnerr := txnFunc(rwtxn)
	if txnerr == nil {
		txnerr = txn.Commit()
		err = txnerr
	} else {
		txn.Abort()
	}
	resultChan <- result
	resultChan <- txnerr
	return err
}

func analyzeDbiStruct(dbiStruct interface{}) (map[string]*DBISettings, error) {
	m := make(map[string]*DBISettings)
	if dbiStruct == nil {
		return m, nil
	}

	t := reflect.TypeOf(dbiStruct)
	if t.Kind() != reflect.Ptr {
		return nil, NotAStructPointer
	}

	t = t.Elem()
	if t.Kind() != reflect.Struct {
		return nil, NotAStructPointer
	}

	dbiSettings := &DBISettings{}
	dbiSettingsType := reflect.TypeOf(dbiSettings)

	v := reflect.ValueOf(dbiStruct)
	v = v.Elem()
	for idx := 0; idx < t.NumField(); idx++ {
		field := t.Field(idx)
		fieldValue := v.Field(idx)
		if dbiSettingsType.AssignableTo(field.Type) && fieldValue.CanSet() &&
			fieldValue.CanInterface() && !fieldValue.IsNil() {
			m[field.Name] = fieldValue.Interface().(*DBISettings)
		}
	}
	return m, nil
}

func (reader *mdbReader) actorLoop() {
	runtime.LockOSThread()
	txnChan := reader.server.readerChan
	var err error
	terminate := false
	for !terminate {
		select {
		case direct := <-reader.queryChan:
			switch direct.code {
			case queryShutdown:
				direct.resultChan <- nil
				terminate = true
			default:
				err = UnexpectedMessage
			}
		case txn := <-txnChan:
			switch txn.code {
			case queryRunTxn:
				err = reader.handleRunTxn(txn.payload.(ReadonlyTransaction), txn.resultChan)
			default:
				err = UnexpectedMessage
			}
		}
		terminate = terminate || err != nil
	}
	if err != nil {
		log.Println(err)
	}
}

func (reader *mdbReader) handleRunTxn(txnFunc ReadonlyTransaction, resultChan chan<- interface{}) error {
	txn, err := reader.server.env.BeginTxn(nil, mdb.RDONLY)
	if err != nil {
		resultChan <- nil
		resultChan <- err
		return err
	}
	rtxn := reader.rtxn
	rtxn.txn = txn
	result, err := txnFunc(rtxn)
	resultChan <- result
	resultChan <- err
	txn.Abort()
	return nil
}

type RTxn struct {
	txn *mdb.Txn
}

func (rtxn *RTxn) Reset()                                           { rtxn.txn.Reset() }
func (rtxn *RTxn) Renew() error                                     { return rtxn.txn.Renew() }
func (rtxn *RTxn) Get(dbi *DBISettings, key []byte) ([]byte, error) { return rtxn.txn.Get(dbi.dbi, key) }
func (rtxn *RTxn) GetVal(dbi *DBISettings, key []byte) (mdb.Val, error) {
	return rtxn.txn.GetVal(dbi.dbi, key)
}

type RWTxn struct {
	RTxn
}

func (rwtxn *RWTxn) Drop(dbi *DBISettings, del int) error { return rwtxn.txn.Drop(dbi.dbi, del) }
func (rwtxn *RWTxn) Put(dbi *DBISettings, key, val []byte, flags uint) error {
	return rwtxn.txn.Put(dbi.dbi, key, val, flags)
}
func (rwtxn *RWTxn) Del(dbi *DBISettings, key, val []byte) error {
	return rwtxn.txn.Del(dbi.dbi, key, val)
}

type TransactionFuture struct {
	Result     interface{}
	Error      error
	resultChan <-chan interface{}
}

func (tf *TransactionFuture) Force() *TransactionFuture {
	if tf.resultChan != nil {
		tf.Result = <-tf.resultChan
		err := <-tf.resultChan
		if err != nil {
			tf.Error = err.(error)
		}
		tf.resultChan = nil
	}
	return tf
}
