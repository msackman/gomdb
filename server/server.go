package server

import (
	"errors"
	mdb "github.com/msackman/gomdb"
	"log"
	"reflect"
	"runtime"
	"time"
)

type ReadonlyTransaction func(rtxn *RTxn) (interface{}, error)
type ReadWriteTransaction func(rwtxn *RWTxn) (interface{}, error)

type DBISettings struct {
	Flags uint
	dbi   mdb.DBI
}

type MDBServer struct {
	writerChan  chan *mdbQuery
	readerChan  chan *mdbQuery
	readers     []*mdbReader
	env         *mdb.Env
	rwtxn       *RWTxn
	batchedTxn  []*readWriteTransactionFuture
	txn         *mdb.Txn
	ticker      *time.Ticker
	txnDuration time.Duration
	zeroDBI     mdb.DBI
}

type mdbReader struct {
	queryChan chan *mdbQuery
	server    *MDBServer
	rtxn      *RTxn
}

type mdbQuery struct {
	code    int
	payload interface{}
}

const (
	queryShutdown = iota
	queryRunTxn   = iota
	queryCommit   = iota
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
		batchedTxn: make([]*readWriteTransactionFuture, 0, defaultChanSize),
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
	txnFuture := newReadonlyTransactionFuture(txn)
	mdb.readerChan <- &mdbQuery{
		code:    queryRunTxn,
		payload: txnFuture,
	}
	return txnFuture
}

func (mdb *MDBServer) ReadWriteTransaction(forceCommit bool, txn ReadWriteTransaction) TransactionFuture {
	txnFuture := newReadWriteTransactionFuture(txn, forceCommit)
	mdb.writerChan <- &mdbQuery{
		code:    queryRunTxn,
		payload: txnFuture,
	}
	return txnFuture
}

func (mdb *MDBServer) Shutdown() {
	mdb.writerChan <- &mdbQuery{
		code: queryShutdown,
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
	initResult <- nil
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
}

func (server *MDBServer) handleQuery(query *mdbQuery) (terminate bool, err error) {
	switch query.code {
	case queryShutdown:
		terminate = true
	case queryRunTxn:
		err = server.handleRunTxn(query.payload.(*readWriteTransactionFuture))
	case queryCommit:
		err = server.commitTxns()
	default:
		err = UnexpectedMessage
	}
	return
}

func (server *MDBServer) handleShutdown() {
	c := make(chan interface{}, 0)
	shutdown := &mdbQuery{
		code:    queryShutdown,
		payload: c,
	}
	for _, reader := range server.readers {
		reader.queryChan <- shutdown
		<-c // await death
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
		if err = env.SetMaxDBs(1 + mdb.DBI(l)); err != nil {
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
	zeroDBI, err := txn.DBIOpen(nil, 0)
	if err != nil {
		txn.Abort()
		return err
	}
	server.zeroDBI = zeroDBI
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

	if err = server.calibrate(); err != nil {
		return err
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

func (server *MDBServer) calibrate() error {
	key := []byte("calibration")
	value := []byte("  testing testing 1 2 3")
	start := time.Now()
	end := start
	count := 0
	hundredMillis := 100 * time.Millisecond
	for count < 50 {
		value[0] = byte(count)
		txn, err := server.env.BeginTxn(nil, 0)
		if txn.Put(server.zeroDBI, key, value, 0); err != nil {
			txn.Abort()
			return err
		}
		if err = txn.Commit(); err != nil {
			return err
		}
		count++
		end = time.Now()
		if end.Sub(start) > hundredMillis {
			break
		}
	}
	txn, err := server.env.BeginTxn(nil, 0)
	if err != nil {
		return err
	}
	if err = txn.Del(server.zeroDBI, key, nil); err != nil {
		txn.Abort()
		return err
	}
	if err = txn.Commit(); err != nil {
		return err
	}
	server.txnDuration = time.Duration(float64(2*end.Sub(start).Nanoseconds()) / float64(count))
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
	txnFuture.result, txnErr = txnFuture.txn(rwtxn)
	if txnErr == nil {
		if txnFuture.forceCommit {
			err = txn.Commit()
			server.txnsComplete(err)
		} else {
			server.ensureTicker()
		}
	} else {
		txn.Abort()
		server.txnsComplete(txnErr)
	}
	return err
}

func (server *MDBServer) txnsComplete(err error) {
	server.txn = nil
	server.cancelTicker()
	for _, txnFuture := range server.batchedTxn {
		txnFuture.error = err
		txnFuture.syncChan <- nil
	}
	server.batchedTxn = server.batchedTxn[:0]
}

func (server *MDBServer) ensureTicker() {
	ticker := time.NewTicker(server.txnDuration)
	server.ticker = ticker
	go func() {
		query := &mdbQuery{code: queryCommit}
		for {
			_, ok := <-ticker.C
			if !ok {
				return
			}
			server.writerChan <- query
		}
	}()
}

func (server *MDBServer) cancelTicker() {
	if server.ticker != nil {
		server.ticker.Stop()
		server.ticker = nil
	}
}

func (server *MDBServer) commitTxns() error {
	if server.txn == nil {
		server.cancelTicker()
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
			switch direct.code {
			case queryShutdown:
				direct.payload.(chan interface{}) <- nil
				terminate = true
			default:
				err = UnexpectedMessage
			}
		case txn := <-txnChan:
			switch txn.code {
			case queryRunTxn:
				err = reader.handleRunTxn(txn.payload.(*readonlyTransactionFuture))
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
	txn, err := reader.server.env.BeginTxn(nil, mdb.RDONLY)
	if err != nil {
		txnFuture.error = err
		txnFuture.syncChan <- nil
		return err
	}
	rtxn := reader.rtxn
	rtxn.txn = txn
	txnFuture.result, txnFuture.error = txnFuture.txn(rtxn)
	txn.Abort()
	txnFuture.syncChan <- nil
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

type TransactionFuture interface {
	Force() TransactionFuture
	Result() interface{}
	Error() error
}

type plainTransactionFuture struct {
	result   interface{}
	error    error
	syncChan chan interface{}
}

func (tf *plainTransactionFuture) Force() TransactionFuture {
	if tf.syncChan != nil {
		<-tf.syncChan
		tf.syncChan = nil
	}
	return tf
}

func (tf *plainTransactionFuture) Result() interface{} {
	tf.Force()
	return tf.result
}

func (tf *plainTransactionFuture) Error() error {
	tf.Force()
	return tf.error
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

func newPlainTransactionFuture() plainTransactionFuture {
	return plainTransactionFuture{syncChan: make(chan interface{}, 1)}
}

func newReadonlyTransactionFuture(txn ReadonlyTransaction) *readonlyTransactionFuture {
	return &readonlyTransactionFuture{
		plainTransactionFuture: newPlainTransactionFuture(),
		txn: txn,
	}
}

func newReadWriteTransactionFuture(txn ReadWriteTransaction, forceCommit bool) *readWriteTransactionFuture {
	return &readWriteTransactionFuture{
		plainTransactionFuture: newPlainTransactionFuture(),
		txn:         txn,
		forceCommit: forceCommit,
	}
}
