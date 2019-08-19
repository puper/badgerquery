package badgerquery

import (
	"github.com/dgraph-io/badger"
)

func Open(opt badger.Options) (*DB, error) {
	badgerDB, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}
	db := &DB{
		tables: map[string]*Table{},
		db:     badgerDB,
	}
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
