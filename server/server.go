package server

import (
	"errors"
	mdb "github.com/msackman/gomdb"
	"log"
	"reflect"
	"runtime"
	"sync"
	"time"
)

type ReadonlyTransaction func(rtxn *RTxn) (interface{}, error)
type ReadWriteTransaction func(rwtxn *RWTxn) (interface{}, error)

type DBISettings struct {
	Flags uint
	dbi   mdb.DBI
}

type MDBServer struct {
	writerChan  chan mdbQuery
	readerChan  chan mdbQuery
	terminated  chan struct{}
	readers     []*mdbReader
	env         *mdb.Env
	rwtxn       *RWTxn
	batchedTxn  []*readWriteTransactionFuture
	txn         *mdb.Txn
	ticker      *time.Ticker
	txnDuration time.Duration
}

type mdbReader struct {
	queryChan chan mdbQuery
	server    *MDBServer
	rtxn      *RTxn
}

type mdbQuery interface {
	mdbQueryWitness()
}

type queryShutdown struct{}
type queryInternalShutdown sync.WaitGroup

func (qs *queryShutdown) mdbQueryWitness()                {}
func (qis *queryInternalShutdown) mdbQueryWitness()       {}
func (rotf *readonlyTransactionFuture) mdbQueryWitness()  {}
func (rwtf *readWriteTransactionFuture) mdbQueryWitness() {}

const (
	defaultChanSize = 64
)

var (
	NotAStructPointer = errors.New("Not a pointer to a struct")
	UnexpectedMessage = errors.New("Unexpected message")
	shutdownQuery     = &queryShutdown{}
)

func NewMDBServer(path string, openFlags, filemode uint, mapSize uint64, numReaders int, commitLatency time.Duration, dbiStruct interface{}) (*MDBServer, error) {
	writerChan := make(chan mdbQuery, defaultChanSize)
	readerChan := make(chan mdbQuery, defaultChanSize)
	if numReaders < 1 {
		numReaders = runtime.GOMAXPROCS(0) / 2 // with 0, just returns current value
		if numReaders < 1 {
			numReaders = 1
		}
	}
	server := &MDBServer{
		writerChan:  writerChan,
		readerChan:  readerChan,
		terminated:  make(chan struct{}),
		readers:     make([]*mdbReader, numReaders),
		rwtxn:       &RWTxn{},
		batchedTxn:  make([]*readWriteTransactionFuture, 0, defaultChanSize),
		txnDuration: commitLatency,
	}
	resultChan := make(chan error, 0)
	go server.actor(path, openFlags, filemode, mapSize, dbiStruct, resultChan)
	result := <-resultChan
	if result == nil {
		return server, nil
	} else {
		return nil, result
	}
}

func (mdb *MDBServer) ReadonlyTransaction(txn ReadonlyTransaction) TransactionFuture {
	txnFuture := newReadonlyTransactionFuture(txn, mdb)
	select {
	case mdb.readerChan <- txnFuture:
	case <-mdb.terminated:
	}
	return txnFuture
}

func (mdb *MDBServer) ReadWriteTransaction(forceCommit bool, txn ReadWriteTransaction) TransactionFuture {
	txnFuture := newReadWriteTransactionFuture(txn, forceCommit, mdb)
	select {
	case mdb.writerChan <- txnFuture:
	case <-mdb.terminated:
	}
	return txnFuture
}

func (mdb *MDBServer) Shutdown() {
	select {
	case mdb.writerChan <- shutdownQuery:
		<-mdb.terminated
	case <-mdb.terminated:
	}
}

func (server *MDBServer) actor(path string, flags, mode uint, mapSize uint64, dbiStruct interface{}, initResult chan<- error) {
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
	close(initResult)
	server.actorLoop()
}

func (server *MDBServer) actorLoop() {
	var err error
	terminate := false
	for !terminate {
		if server.txn == nil {
			query := <-server.writerChan
			terminate, err = server.handleQuery(query)
		} else {
			select {
			case query := <-server.writerChan:
				terminate, err = server.handleQuery(query)
			case <-server.ticker.C:
				err = server.commitTxns()
			default:
				err = server.commitTxns()
			}
		}
		terminate = terminate || err != nil
	}
	if err != nil {
		log.Println(err)
	}
	if err = server.commitTxns(); err != nil {
		log.Println(err)
	}
	server.handleShutdown()
	close(server.terminated)
}

func (server *MDBServer) handleQuery(query mdbQuery) (terminate bool, err error) {
	switch msg := query.(type) {
	case *queryShutdown:
		terminate = true
	case *readWriteTransactionFuture:
		err = server.handleRunTxn(msg)
	default:
		err = UnexpectedMessage
	}
	return
}

func (server *MDBServer) handleShutdown() {
	wg := new(sync.WaitGroup)
	wg.Add(len(server.readers))
	is := (*queryInternalShutdown)(wg)
	for _, reader := range server.readers {
		reader.queryChan <- is
	}
	wg.Wait()
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

	for idx := range server.readers {
		reader := &mdbReader{
			queryChan: make(chan mdbQuery, defaultChanSize),
			server:    server,
			rtxn:      &RTxn{},
		}
		server.readers[idx] = reader
		go reader.actorLoop()
	}
	return nil
}

func (server *MDBServer) handleRunTxn(txnFuture *readWriteTransactionFuture) error {
	/*
		If creating a txn, or commiting a txn errors, then that kills both the txns and us.
			If the txn func itself errors, that kills the txns, but it doesn't kill us.
	*/
	var err error
	server.batchedTxn = append(server.batchedTxn, txnFuture)
	txn := server.txn
	if txn == nil {
		txn, err = server.env.BeginTxn(nil, 0)
		if err != nil {
			server.txnsComplete(err)
			return err
		}
		server.txn = txn
	}
	rwtxn := server.rwtxn
	rwtxn.txn = txn
	var txnErr error
	if !txnFuture.forceCommit {
		server.ensureTicker()
	}
	txnFuture.result, txnErr = txnFuture.txn(rwtxn)
	if txnErr == nil {
		if txnFuture.forceCommit {
			server.commitTxns()
		}
	} else {
		txn.Abort()
		server.txnsComplete(txnErr)
	}
	return err
}

func (server *MDBServer) txnsComplete(err error) {
	server.txn = nil
	for _, txnFuture := range server.batchedTxn {
		txnFuture.error = err
		close(txnFuture.signal)
	}
	server.cancelTicker()
	server.batchedTxn = server.batchedTxn[:0]
}

func (server *MDBServer) ensureTicker() {
	if server.ticker == nil {
		ticker := time.NewTicker(server.txnDuration)
		server.ticker = ticker
	}
}

func (server *MDBServer) cancelTicker() {
	if server.ticker != nil {
		server.ticker.Stop()
		server.ticker = nil
	}
}

func (server *MDBServer) commitTxns() error {
	if server.txn == nil {
		return nil
	} else {
		err := server.txn.Commit()
		server.txnsComplete(err)
		return err
	}
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
			switch query := direct.(type) {
			case *queryInternalShutdown:
				((*sync.WaitGroup)(query)).Done()
				terminate = true
			default:
				err = UnexpectedMessage
			}
		case query := <-txnChan:
			switch msg := query.(type) {
			case *readonlyTransactionFuture:
				err = reader.handleRunTxn(msg)
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

func (reader *mdbReader) handleRunTxn(txnFuture *readonlyTransactionFuture) error {
	defer close(txnFuture.signal)
	txn, err := reader.server.env.BeginTxn(nil, mdb.RDONLY)
	if err != nil {
		txnFuture.error = err
		return err
	}
	rtxn := reader.rtxn
	rtxn.txn = txn
	txnFuture.result, txnFuture.error = txnFuture.txn(rtxn)
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
func (rtxn *RTxn) WithCursor(dbi *DBISettings, fun func(cursor *mdb.Cursor) (interface{}, error)) (interface{}, error) {
	cursor, err := rtxn.txn.CursorOpen(dbi.dbi)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	return fun(cursor)
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

type TransactionFuture interface {
	Force() TransactionFuture
	ResultError() (interface{}, error)
}

type plainTransactionFuture struct {
	result     interface{}
	error      error
	signal     chan struct{}
	terminated chan struct{}
}

func (tf *plainTransactionFuture) Force() TransactionFuture {
	select {
	case <-tf.signal:
	case <-tf.terminated:
	}
	return tf
}

func (tf *plainTransactionFuture) ResultError() (interface{}, error) {
	tf.Force()
	return tf.result, tf.error
}

type readonlyTransactionFuture struct {
	plainTransactionFuture
	txn ReadonlyTransaction
}

type readWriteTransactionFuture struct {
	plainTransactionFuture
	txn         ReadWriteTransaction
	forceCommit bool
}

func newPlainTransactionFuture(mdb *MDBServer) plainTransactionFuture {
	return plainTransactionFuture{
		signal:     make(chan struct{}),
		terminated: mdb.terminated,
	}
}

func newReadonlyTransactionFuture(txn ReadonlyTransaction, mdb *MDBServer) *readonlyTransactionFuture {
	return &readonlyTransactionFuture{
		plainTransactionFuture: newPlainTransactionFuture(mdb),
		txn: txn,
	}
}

func newReadWriteTransactionFuture(txn ReadWriteTransaction, forceCommit bool, mdb *MDBServer) *readWriteTransactionFuture {
	return &readWriteTransactionFuture{
		plainTransactionFuture: newPlainTransactionFuture(mdb),
		txn:         txn,
		forceCommit: forceCommit,
	}
}
