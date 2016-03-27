package server

import (
	"errors"
	cc "github.com/msackman/chancell"
	mdb "github.com/msackman/gomdb"
	"log"
	"reflect"
	"runtime"
	"sync"
	"syscall"
	"time"
)

type ReadonlyTransaction func(rtxn *RTxn) interface{}
type ReadWriteTransaction func(rwtxn *RWTxn) interface{}

type DBISettings struct {
	Flags uint
	dbi   mdb.DBI
	name  string
}

func (dbis *DBISettings) Clone() *DBISettings {
	if dbis == nil {
		return nil
	} else {
		return &DBISettings{Flags: dbis.Flags}
	}
}

type DBIsInterface interface {
	SetServer(*MDBServer)
	Clone() DBIsInterface
}

type MDBServer struct {
	writerCellTail          *cc.ChanCellTail
	writerEnqueueQueryInner func(mdbQuery, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	writerChan              <-chan mdbQuery
	readerCellTail          *cc.ChanCellTail
	readerEnqueueQueryInner func(mdbQuery, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	readerChan              <-chan mdbQuery
	readers                 []*mdbReader
	env                     *mdb.Env
	rwtxn                   *RWTxn
	batchedTxn              []*readWriteTransactionFuture
	txn                     *mdb.Txn
	ticker                  *time.Ticker
	txnDuration             time.Duration
}

type mdbReader struct {
	server *MDBServer
	rtxn   *RTxn
}

type mdbQuery interface {
	mdbQueryWitness()
}

type queryShutdown struct{}
type queryInternalShutdown sync.WaitGroup
type queryPause struct {
	pause  *sync.WaitGroup
	resume <-chan struct{}
}

func (qs *queryShutdown) mdbQueryWitness()                {}
func (qis *queryInternalShutdown) mdbQueryWitness()       {}
func (qp *queryPause) mdbQueryWitness()                   {}
func (rotf *readonlyTransactionFuture) mdbQueryWitness()  {}
func (rwtf *readWriteTransactionFuture) mdbQueryWitness() {}
func (wef *withEnvFuture) mdbQueryWitness()               {}

var (
	ServerTerminated  = errors.New("Server already terminated")
	NotAStructPointer = errors.New("Not a pointer to a struct")
	UnexpectedMessage = errors.New("Unexpected message")
	shutdownQuery     = &queryShutdown{}
)

func NewMDBServer(path string, openFlags, filemode uint, mapSize uint64, numReaders int, commitLatency time.Duration, dbiStruct DBIsInterface) (DBIsInterface, error) {
	dbiStruct = dbiStruct.Clone()
	if numReaders < 1 {
		numReaders = runtime.GOMAXPROCS(0) / 2 // with 0, just returns current value
		if numReaders < 1 {
			numReaders = 1
		}
	}
	server := &MDBServer{
		readers:     make([]*mdbReader, numReaders),
		rwtxn:       &RWTxn{},
		batchedTxn:  make([]*readWriteTransactionFuture, 0, 32), // MAGIC NUMBER
		txnDuration: commitLatency,
	}

	var writerHead *cc.ChanCellHead
	writerHead, server.writerCellTail = cc.NewChanCellTail(
		func(n int, cell *cc.ChanCell) {
			queryChan := make(chan mdbQuery, n)
			cell.Open = func() { server.writerChan = queryChan }
			cell.Close = func() { close(queryChan) }
			server.writerEnqueueQueryInner = func(msg mdbQuery, curCell *cc.ChanCell, cont cc.CurCellConsumer) (bool, cc.CurCellConsumer) {
				if curCell == cell {
					select {
					case queryChan <- msg:
						return true, nil
					default:
						return false, nil
					}
				} else {
					return false, cont
				}
			}
		})

	var readerHead *cc.ChanCellHead
	readerHead, server.readerCellTail = cc.NewChanCellTail(
		func(n int, cell *cc.ChanCell) {
			queryChan := make(chan mdbQuery, n)
			cell.Open = func() { server.readerChan = queryChan }
			cell.Close = func() { close(queryChan) }
			server.readerEnqueueQueryInner = func(msg mdbQuery, curCell *cc.ChanCell, cont cc.CurCellConsumer) (bool, cc.CurCellConsumer) {
				if curCell == cell {
					select {
					case queryChan <- msg:
						return true, nil
					default:
						return false, nil
					}
				} else {
					return false, cont
				}
			}
		})

	resultChan := make(chan error, 0)
	go server.actor(path, openFlags, filemode, mapSize, dbiStruct, resultChan, writerHead, readerHead)
	result := <-resultChan
	if result == nil {
		dbiStruct.SetServer(server)
		return dbiStruct, nil
	} else {
		return nil, result
	}
}

func (mdb *MDBServer) ReadonlyTransaction(txn ReadonlyTransaction) TransactionFuture {
	txnFuture := newReadonlyTransactionFuture(txn, mdb)
	if !mdb.enqueueReader(txnFuture) {
		txnFuture.error = ServerTerminated
		close(txnFuture.signal)
	}
	return txnFuture
}

func (mdb *MDBServer) ReadWriteTransaction(forceCommit bool, txn ReadWriteTransaction) TransactionFuture {
	txnFuture := newReadWriteTransactionFuture(txn, forceCommit, mdb)
	if !mdb.enqueueWriter(txnFuture) {
		txnFuture.error = ServerTerminated
		close(txnFuture.signal)
	}
	return txnFuture
}

func (mdb *MDBServer) WithEnv(fun func(*mdb.Env) (interface{}, error)) TransactionFuture {
	future := newWithEnvFuture(fun, mdb)
	if !mdb.enqueueWriter(future) {
		future.error = ServerTerminated
		close(future.signal)
	}
	return future
}

func (mdb *MDBServer) Shutdown() {
	if mdb.enqueueWriter(shutdownQuery) {
		mdb.writerCellTail.Wait()
	}
}

func (mdb *MDBServer) enqueueReader(msg mdbQuery) bool {
	var f cc.CurCellConsumer
	f = func(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
		return mdb.readerEnqueueQueryInner(msg, cell, f)
	}
	return mdb.readerCellTail.WithCell(f)
}

func (mdb *MDBServer) enqueueWriter(msg mdbQuery) bool {
	var f cc.CurCellConsumer
	f = func(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
		return mdb.writerEnqueueQueryInner(msg, cell, f)
	}
	return mdb.writerCellTail.WithCell(f)
}

func (server *MDBServer) actor(path string, flags, mode uint, mapSize uint64, dbiStruct DBIsInterface, initResult chan<- error, writerHead, readerHead *cc.ChanCellHead) {
	runtime.LockOSThread()
	defer func() {
		if server.env != nil {
			server.env.Close()
		}
	}()
	if err := server.init(path, flags, mode, mapSize, dbiStruct, readerHead); err != nil {
		initResult <- err
		return
	}
	close(initResult)
	server.actorLoop(writerHead)
}

func (server *MDBServer) actorLoop(writerHead *cc.ChanCellHead) {
	var (
		err        error
		writerChan <-chan mdbQuery
		writerCell *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { writerChan, writerCell = server.writerChan, cell }
	writerHead.WithCell(chanFun)
	terminate := false
	for !terminate {
		if server.txn == nil {
			if query, ok := <-writerChan; ok {
				terminate, err = server.handleQuery(query)
			} else {
				writerHead.Next(writerCell, chanFun)
			}
		} else {
			select {
			case query, ok := <-writerChan:
				if ok {
					terminate, err = server.handleQuery(query)
				} else {
					writerHead.Next(writerCell, chanFun)
				}
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
	server.writerCellTail.Terminate()
	server.handleShutdown()
	server.readerCellTail.Terminate()
}

func (server *MDBServer) handleQuery(query mdbQuery) (terminate bool, err error) {
	switch msg := query.(type) {
	case *queryShutdown:
		terminate = true
	case *readWriteTransactionFuture:
		err = server.handleRunTxn(msg)
	case *withEnvFuture:
		err = server.handleWithEnv(msg)
	default:
		err = UnexpectedMessage
	}
	return
}

func (server *MDBServer) handleShutdown() {
	wg := new(sync.WaitGroup)
	wg.Add(len(server.readers))
	is := (*queryInternalShutdown)(wg)
	for range server.readers {
		server.enqueueReader(is)
	}
	wg.Wait()
}

func (server *MDBServer) init(path string, flags, mode uint, mapSize uint64, dbiStruct DBIsInterface, readerHead *cc.ChanCellHead) error {
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

	dbis, err := analyzeDbiStruct(dbiStruct)
	if err != nil {
		return err
	}

	if l := len(dbis); l != 0 {
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
	for _, value := range dbis {
		dbi, err := txn.DBIOpen(&value.name, value.Flags)
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
			server: server,
			rtxn:   &RTxn{},
		}
		server.readers[idx] = reader
		go reader.actorLoop(readerHead)
	}
	return nil
}

func (server *MDBServer) handleWithEnv(future *withEnvFuture) error {
	if err := server.commitTxns(); err != nil {
		future.error = err
		close(future.signal)
		return err
	}
	future.result, future.error = future.fun(server.env)
	close(future.signal)
	return future.error
}

func (server *MDBServer) handleRunTxn(txnFuture *readWriteTransactionFuture) error {
	// Txns can not choose to abort. Thus any "errors" that occur are
	// completely fatal to us.
	var err error
	server.batchedTxn = append(server.batchedTxn, txnFuture)
	txn := server.txn
	if txn == nil {
		txn, err = server.createTxn(0)
		if err != nil {
			server.txnsComplete(err)
			return err
		}
		server.txn = txn
	}
	rwtxn := server.rwtxn
	rwtxn.txn = txn
	rwtxn.error = nil
	txnFuture.result = txnFuture.txn(rwtxn)
	err = rwtxn.error
	switch err {
	case nil:
		if txnFuture.forceCommit {
			err = server.commitTxns()
		} else {
			server.ensureTicker()
		}

	case mdb.MapFull:
		txn.Abort()
		server.txn = nil
		err = server.expandMap()

	default:
		txnFuture.error = err
		txn.Abort()
		server.txn = nil
		server.txnsComplete(err)
	}
	return err
}

func (server *MDBServer) expandMap() error {
	info, err := server.env.Info()
	if err == nil {
		resume := make(chan struct{})
		pauser := &queryPause{
			pause:  new(sync.WaitGroup),
			resume: resume,
		}
		pauser.pause.Add(len(server.readers))
		for range server.readers {
			server.enqueueReader(pauser)
		}
		pauser.pause.Wait()

		mapSize := info.MapSize * 2
		log.Println("New map size:", mapSize)
		err = server.env.SetMapSize(mapSize)
		close(resume)

		if err == nil {
			txns := make([]*readWriteTransactionFuture, len(server.batchedTxn))
			copy(txns, server.batchedTxn)
			server.batchedTxn = server.batchedTxn[:0]
			for idx, txnFuture := range txns {
				err = server.handleRunTxn(txnFuture)
				if err != nil {
					server.batchedTxn = append(server.batchedTxn, txns[idx+1:]...)
					server.txnsComplete(err)
					break
				}
			}
		}
	}
	return err
}

func (server *MDBServer) createTxn(flags uint) (*mdb.Txn, error) {
	for {
		txn, err := server.env.BeginTxn(nil, flags)
		if err == mdb.MapResized {
			err = server.env.SetMapSize(0)
			if err == nil {
				continue
			}
		}
		return txn, err
	}
}

func (server *MDBServer) txnsComplete(err error) {
	server.txn = nil
	server.cancelTicker()
	for _, txnFuture := range server.batchedTxn {
		txnFuture.error = err
		close(txnFuture.signal)
	}
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
		if err == mdb.MapFull {
			server.txn = nil
			return server.expandMap()

		} else {
			server.txnsComplete(err)
			return err
		}
	}
}

func analyzeDbiStruct(dbiStruct DBIsInterface) ([]*DBISettings, error) {
	l := []*DBISettings{}
	if dbiStruct == nil {
		return l, nil
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
			dbis := fieldValue.Interface().(*DBISettings)
			dbis.name = field.Name
			l = append(l, dbis)
		}
	}
	return l, nil
}

func (reader *mdbReader) actorLoop(readerHead *cc.ChanCellHead) {
	runtime.LockOSThread()
	var (
		readerChan <-chan mdbQuery
		readerCell *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { readerChan, readerCell = reader.server.readerChan, cell }
	readerHead.WithCell(chanFun)

	txn, err := reader.server.createTxn(mdb.RDONLY)
	if err == nil {
		reader.rtxn.txn = txn
	}
	terminate := err != nil
	for !terminate {
		if query, ok := <-readerChan; ok {
			switch msg := query.(type) {
			case *readonlyTransactionFuture:
				err = reader.handleRunTxn(msg)
			case *queryPause:
				txn := reader.rtxn.txn
				if txn != nil {
					txn.Abort()
					reader.rtxn.txn = nil
				}
				msg.pause.Done()
				<-msg.resume
			case *queryInternalShutdown:
				((*sync.WaitGroup)(msg)).Done()
				terminate = true
			default:
				err = UnexpectedMessage
			}
		} else {
			readerHead.Next(readerCell, chanFun)
		}
		terminate = terminate || err != nil
	}
	if err != nil {
		log.Println(err)
	}
}

func (reader *mdbReader) handleRunTxn(txnFuture *readonlyTransactionFuture) error {
	defer close(txnFuture.signal)
	rtxn := reader.rtxn
	var err error
	if rtxn.txn != nil {
		err = rtxn.txn.Renew()
		if err == mdb.Invalid || err == syscall.EINVAL {
			rtxn.txn.Abort()
			rtxn.txn = nil
		}
	}
	if rtxn.txn == nil {
		rtxn.txn, err = reader.server.createTxn(mdb.RDONLY)
	}
	if err != nil {
		rtxn.txn = nil
		txnFuture.error = err
		return err
	}

	rtxn.error = nil
	txnFuture.result = txnFuture.txn(rtxn)
	txnFuture.error = rtxn.error
	rtxn.txn.Reset()
	return nil
}

type RTxn struct {
	txn   *mdb.Txn
	error error
}

func (rtxn *RTxn) Error(err error) error {
	if rtxn.error == nil {
		rtxn.error = err
	}
	return rtxn.error
}

func (rtxn *RTxn) Reset() error {
	if rtxn.error == nil {
		rtxn.txn.Reset()
	}
	return rtxn.error
}

func (rtxn *RTxn) Renew() error {
	if rtxn.error == nil {
		rtxn.error = rtxn.txn.Renew()
	}
	return rtxn.error
}

func (rtxn *RTxn) Get(dbi *DBISettings, key []byte) ([]byte, error) {
	if rtxn.error == nil {
		result, err := rtxn.txn.Get(dbi.dbi, key)
		if err != nil && err != mdb.NotFound {
			rtxn.error = err
		}
		return result, err
	} else {
		return nil, rtxn.error
	}
}

// Do NOT call Free() on the result *mdb.Val
func (rtxn *RTxn) GetVal(dbi *DBISettings, key []byte) (*mdb.Val, error) {
	if rtxn.error == nil {
		result, err := rtxn.txn.GetVal(dbi.dbi, key)
		if err != nil && err != mdb.NotFound {
			rtxn.error = err
		}
		return result, err
	} else {
		return nil, rtxn.error
	}
}

func (rtxn *RTxn) WithCursor(dbi *DBISettings, fun func(cursor *Cursor) interface{}) (interface{}, error) {
	if rtxn.error == nil {
		cursor, err := rtxn.txn.CursorOpen(dbi.dbi)
		if err != nil {
			rtxn.error = err
			return nil, err
		}
		defer cursor.Close()
		return fun(&Cursor{RTxn: rtxn, cursor: cursor}), nil
	}
	return nil, rtxn.error
}

type Cursor struct {
	*RTxn
	cursor *mdb.Cursor
}

func (c *Cursor) Get(key, val []byte, op uint) ([]byte, []byte, error) {
	if c.error == nil {
		rkey, rVal, err := c.cursor.Get(key, val, op)
		if err != nil && err != mdb.NotFound {
			c.error = err
		}
		return rkey, rVal, err
	}
	return nil, nil, c.error
}

// Caller's responsibility to call Free() on return key and value iff non-nil
func (c *Cursor) GetVal(key, val []byte, op uint) (*mdb.Val, *mdb.Val, error) {
	if c.error == nil {
		rkey, rVal, err := c.cursor.GetVal(key, val, op)
		if err != nil && err != mdb.NotFound {
			c.error = err
		}
		return rkey, rVal, err
	}
	return nil, nil, c.error
}

func (c *Cursor) Put(key, val []byte, flags uint) error {
	if c.error == nil {
		c.error = c.cursor.Put(key, val, flags)
	}
	return c.error
}

func (c *Cursor) Del(flags uint) error {
	if c.error == nil {
		err := c.cursor.Del(flags)
		if err != nil && err != mdb.NotFound {
			c.error = err
		}
		return err
	}
	return c.error
}

type RWTxn struct {
	RTxn
}

func (rwtxn *RWTxn) Drop(dbi *DBISettings, del int) error {
	if rwtxn.error == nil {
		rwtxn.error = rwtxn.txn.Drop(dbi.dbi, del)
	}
	return rwtxn.error
}

func (rwtxn *RWTxn) Put(dbi *DBISettings, key, val []byte, flags uint) error {
	if rwtxn.error == nil {
		rwtxn.error = rwtxn.txn.Put(dbi.dbi, key, val, flags)
	}
	return rwtxn.error
}

func (rwtxn *RWTxn) Del(dbi *DBISettings, key, val []byte) error {
	if rwtxn.error == nil {
		err := rwtxn.txn.Del(dbi.dbi, key, val)
		if err != nil && err != mdb.NotFound {
			rwtxn.error = err
		}
		return err
	}
	return rwtxn.error
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

type withEnvFuture struct {
	plainTransactionFuture
	fun func(*mdb.Env) (interface{}, error)
}

func newPlainTransactionFuture(terminated chan struct{}) plainTransactionFuture {
	return plainTransactionFuture{
		signal:     make(chan struct{}),
		terminated: terminated,
	}
}

func newReadonlyTransactionFuture(txn ReadonlyTransaction, mdb *MDBServer) *readonlyTransactionFuture {
	return &readonlyTransactionFuture{
		plainTransactionFuture: newPlainTransactionFuture(mdb.readerCellTail.Terminated),
		txn: txn,
	}
}

func newReadWriteTransactionFuture(txn ReadWriteTransaction, forceCommit bool, mdb *MDBServer) *readWriteTransactionFuture {
	return &readWriteTransactionFuture{
		plainTransactionFuture: newPlainTransactionFuture(mdb.writerCellTail.Terminated),
		txn:         txn,
		forceCommit: forceCommit,
	}
}

func newWithEnvFuture(fun func(*mdb.Env) (interface{}, error), mdb *MDBServer) *withEnvFuture {
	return &withEnvFuture{
		plainTransactionFuture: newPlainTransactionFuture(mdb.writerCellTail.Terminated),
		fun: fun,
	}
}
