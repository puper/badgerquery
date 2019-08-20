package badgerquery

import (
	"context"
	"time"

	"github.com/dgraph-io/badger"
)

type Options struct {
	GCFreq   time.Duration
	SyncFreq time.Duration
}

func Open(badgerOpt badger.Options, opt Options) (*DB, error) {
	badgerDB, err := badger.Open(badgerOpt)
	if err != nil {
		return nil, err
	}
	ctxBack := context.Background()
	ctx, cancel := context.WithCancel(ctxBack)
	db := &DB{
		tables: map[string]*Table{},
		db:     badgerDB,
		ctx:    ctx,
		cancel: cancel,
	}
	if !badgerOpt.SyncWrites {
		db.wg.Add(1)
		go db.syncLoop(opt.SyncFreq)
	}
	// 启动定时gc
	db.wg.Add(1)
	go db.gcLoop(opt.GCFreq)
	defer func() {
		if err != nil {
			db.Close()
		}
	}()
	err = db.loadMeta()
	if err != nil {
		return nil, err
	}
	db.seq, err = badgerDB.GetSequence(getMetaSeqKey(), 100)
	if err != nil {
		return nil, err
	}
	return db, err
}
