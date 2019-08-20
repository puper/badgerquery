package badgerquery

import (
	"bytes"
	"context"
	"log"
	"sync"
	"time"

	"github.com/google/orderedcode"

	"github.com/dgraph-io/badger"
)

type DB struct {
	tableLock sync.RWMutex
	tables    map[string]*Table
	db        *badger.DB
	seq       *badger.Sequence
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

func (this *DB) loadMeta() error {
	this.tableLock.Lock()
	defer this.tableLock.Unlock()
	err := this.db.View(func(txn *badger.Txn) error {
		var (
			err error
		)
		opt := badger.DefaultIteratorOptions
		opt.Prefix = tableConfigKeyBase
		iter := txn.NewIterator(opt)
		defer iter.Close()
		for iter.Rewind(); iter.ValidForPrefix(opt.Prefix); iter.Next() {
			var tableConfig *TableConfig
			iter.Item().Value(func(val []byte) error {
				tableConfig, err = DecodeTableConfig(val)
				return err
			})
			if err != nil {
				return err
			}
			this.tables[tableConfig.Name] = &Table{
				config:  tableConfig,
				indexes: map[string]*Index{},
			}
		}
		opt.Prefix = indexConfigKeyBase
		indexIter := txn.NewIterator(opt)
		defer indexIter.Close()
		for indexIter.Rewind(); indexIter.ValidForPrefix(opt.Prefix); indexIter.Next() {
			var indexConfig *IndexConfig
			indexIter.Item().Value(func(val []byte) error {
				indexConfig, err = DecodeIndexConfig(val)
				return err
			})
			if err != nil {
				return err
			}
			indexSeq, err := this.db.GetSequence(getIndexSeqKey(indexConfig.TableName, indexConfig.Name), 100)
			if err != nil {
				return err
			}
			this.tables[indexConfig.TableName].indexes[indexConfig.Name] = &Index{
				config: indexConfig,
				seq:    indexSeq,
			}
		}
		return nil
	})
	return err
}

func (this *DB) CreateTable(tableName string, indexConfigs map[string]*IndexConfig) error {
	this.tableLock.Lock()
	defer this.tableLock.Unlock()
	if _, ok := this.tables[tableName]; ok {
		return ErrTableExists
	}
	tableConfig := &TableConfig{}
	indexes := map[string]*Index{}
	err := this.Update(func(txn *badger.Txn) error {
		tableConfig = &TableConfig{}
		for _, index := range indexes {
			index.Close()
		}
		indexes = map[string]*Index{}
		var (
			err error
		)
		tableConfig.Name = tableName
		tableConfig.DataID, err = this.NextID(this.seq)
		if err != nil {
			return err
		}
		tableConfig.IndexID, err = this.NextID(this.seq)
		if err != nil {
			return err
		}
		err = txn.Set(getTableConfigKey(tableName), EncodeTableConfig(tableConfig))
		if err != nil {
			return err
		}
		for indexName, indexConfig := range indexConfigs {
			indexConfig.TableName = tableName
			indexConfig.Name = indexName
			indexConfig.ID, err = this.NextID(this.seq)
			if err != nil {
				return err
			}
			txn.Set(getIndexConfigKey(tableName, indexName), EncodeIndexConfig(indexConfig))
			indexSeq, err := this.db.GetSequence(getIndexSeqKey(tableName, indexName), 100)
			if err != nil {
				return err
			}
			indexes[indexName] = &Index{
				config: indexConfig,
				seq:    indexSeq,
			}
		}
		return nil
	}, nil)
	if err != nil {
		return err
	}
	this.tables[tableName] = &Table{
		config:  tableConfig,
		indexes: indexes,
	}
	return nil
}

// not safe
func (this *DB) CreateItem(item Item) error {
	this.tableLock.RLock()
	defer this.tableLock.RUnlock()
	err := this.Update(func(txn *badger.Txn) error {
		return this.createItem(txn, item)
	}, nil)
	return err
}

// not safe
func (this *DB) UpdateItem(item Item) error {
	this.tableLock.RLock()
	defer this.tableLock.RUnlock()
	err := this.Update(func(txn *badger.Txn) error {
		return this.updateItem(txn, item)
	}, nil)
	return err
}

// safe
func (this *DB) SaveItem(item Item) error {
	this.tableLock.RLock()
	defer this.tableLock.RUnlock()
	table, ok := this.tables[item.TableName()]
	if !ok {
		return ErrTableNotExists
	}
	err := this.Update(func(txn *badger.Txn) error {
		has, err := this.hasItem(txn, table.config.DataID, item.Key())
		if err != nil {
			return err
		}
		if has {
			return this.updateItem(txn, item)
		}
		return this.createItem(txn, item)
	}, nil)
	return err
}

// safe
func (this *DB) DeleteItem(tableName string, itemKey string) error {
	this.tableLock.RLock()
	defer this.tableLock.RUnlock()
	table, ok := this.tables[tableName]
	if !ok {
		return ErrTableNotExists
	}
	err := this.Update(func(txn *badger.Txn) error {
		return this.deleteItem(txn, table, itemKey)
	}, nil)
	return err
}

func (this *DB) GetItem(itemKey string, item Item) error {
	this.tableLock.RLock()
	defer this.tableLock.RUnlock()
	table, ok := this.tables[item.TableName()]
	if !ok {
		return ErrTableNotExists
	}
	err := this.View(func(txn *badger.Txn) error {
		err := this.getItemData(txn, table.config.DataID, itemKey, item)
		return err
	}, nil)
	return err
}

func (this *DB) HasItem(tableName string, itemKey string) (bool, error) {
	this.tableLock.RLock()
	defer this.tableLock.RUnlock()
	table, ok := this.tables[tableName]
	if !ok {
		return false, ErrTableNotExists
	}
	var (
		has bool
		err error
	)
	this.View(func(txn *badger.Txn) error {
		has, err = this.hasItem(txn, table.config.DataID, itemKey)
		return err
	}, nil)
	return has, err
}

func (this *DB) hasItem(txn *badger.Txn, tableDataID uint64, itemKey string) (bool, error) {
	_, err := txn.Get(getItemDataKey(tableDataID, itemKey))
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (this *DB) createItem(txn *badger.Txn, item Item) error {
	var (
		err error
	)
	tableName := item.TableName()
	table, ok := this.tables[tableName]
	if !ok {
		return ErrTableNotExists
	}
	tableDataID := table.config.DataID
	tableIndexID := table.config.IndexID
	itemKey := item.Key()
	err = this.setItemData(txn, tableDataID, itemKey, item.Marshal())
	if err != nil {
		return err
	}
	itemIndexes := this.getCurrentItemIndexes(table, item)
	for indexName, indexData := range itemIndexes {
		index, ok := table.indexes[indexName]
		if !ok {
			continue
		}
		seq, err := this.NextID(index.seq)
		if err != nil {
			return err
		}
		err = this.addIndexData(txn, itemKey, mustEncodeOrderedCode(indexData, seq))
		if err != nil {
			return err
		}
	}
	err = this.setItemIndexes(txn, tableIndexID, itemKey, itemIndexes)
	return err
}

func (this *DB) addIndexData(txn *badger.Txn, itemKey string, indexData []byte) error {
	err := txn.Set(indexData, []byte(itemKey))
	return err
}

func (this *DB) deleteIndexData(txn *badger.Txn, indexData []byte) error {
	err := txn.Delete(indexData)
	return err
}

func (this *DB) updateItem(txn *badger.Txn, item Item) error {
	var (
		err error
	)
	tableName := item.TableName()
	table := this.tables[tableName]
	table, ok := this.tables[tableName]
	if !ok {
		return ErrTableNotExists
	}
	tableDataID := table.config.DataID
	tableIndexID := table.config.IndexID
	itemKey := item.Key()
	err = this.setItemData(txn, tableDataID, itemKey, item.Marshal())
	if err != nil {
		return err
	}
	oldItemIndexes, err := this.getItemIndexes(txn, tableIndexID, itemKey)
	if err == badger.ErrKeyNotFound {
		oldItemIndexes = ItemIndexes{}
	} else if err != nil {
		return err
	}
	currentItemIndexes := this.getCurrentItemIndexes(table, item)
	for indexName, currentIndexData := range currentItemIndexes {
		index, ok := table.indexes[indexName]
		if !ok {
			continue
		}
		if oldIndexData, ok := oldItemIndexes[indexName]; !ok {
			seq, err := this.NextID(index.seq)
			if err != nil {
				return err
			}
			this.addIndexData(txn, itemKey, mustEncodeOrderedCode(currentIndexData, seq))
		} else {
			delete(oldItemIndexes, indexName)
			currentIndexDataMax := mustEncodeOrderedCode(currentIndexData, orderedcode.Infinity)
			if bytes.Compare(oldIndexData, currentIndexDataMax) == 1 || bytes.Compare(oldIndexData, currentIndexData) == -1 {
				seq, err := this.NextID(index.seq)
				if err != nil {
					return err
				}
				err = this.addIndexData(txn, itemKey, mustEncodeOrderedCode(currentIndexData, seq))
				if err != nil {
					return err
				}
				err = this.deleteIndexData(txn, oldIndexData)
				if err != nil && err != badger.ErrKeyNotFound {
					return err
				}
			} else {
			}
		}
	}
	for _, oldIndexData := range oldItemIndexes {
		err = this.deleteIndexData(txn, oldIndexData)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
	}
	err = this.setItemIndexes(txn, tableIndexID, itemKey, currentItemIndexes)
	if err != nil {
		return err
	}
	return nil
}

func (this *DB) deleteItem(txn *badger.Txn, table *Table, itemKey string) error {
	err := this.deleteItemData(txn, table.config.DataID, itemKey)
	if err != nil {
		return err
	}
	itemIndexes, err := this.getItemIndexes(txn, table.config.IndexID, itemKey)
	if err == badger.ErrKeyNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	for _, indexData := range itemIndexes {
		err := this.deleteIndexData(txn, indexData)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
	}
	err = this.deleteItemIndexes(txn, table.config.IndexID, itemKey)
	if err != nil && err != badger.ErrKeyNotFound {
		return err
	}
	return nil
}

func (this *DB) setItemData(txn *badger.Txn, tableDataID uint64, itemKey string, itemBytes []byte) error {
	return txn.Set(getItemDataKey(tableDataID, itemKey), itemBytes)
}

func (this *DB) getItemData(txn *badger.Txn, tableDataID uint64, itemKey string, item Item) error {
	badgerItem, err := txn.Get(getItemDataKey(tableDataID, itemKey))
	if err != nil {
		return err
	}
	return badgerItem.Value(func(val []byte) error {
		return item.Unmarshal(val)
	})
}

func (this *DB) deleteItemData(txn *badger.Txn, tableDataID uint64, itemKey string) error {
	err := txn.Delete(getItemDataKey(tableDataID, itemKey))
	return err
}

func (this *DB) setItemIndexes(txn *badger.Txn, tableIndexID uint64, itemKey string, itemIndexes ItemIndexes) error {
	return txn.Set(getItemIndexKey(tableIndexID, itemKey), itemIndexes.Marshal())
}

func (this *DB) getItemIndexes(txn *badger.Txn, tableIndexID uint64, itemKey string) (ItemIndexes, error) {
	badgerItem, err := txn.Get(getItemIndexKey(tableIndexID, itemKey))
	if err != nil {
		return nil, err
	}
	itemIndexes := ItemIndexes{}
	err = badgerItem.Value(func(val []byte) error {
		return itemIndexes.Unmarshal(val)
	})
	if err != nil {
		return nil, err
	}
	return itemIndexes, nil
}

func (this *DB) deleteItemIndexes(txn *badger.Txn, tableIndexID uint64, itemKey string) error {
	err := txn.Delete(getItemIndexKey(tableIndexID, itemKey))
	return err
}

// 如果索引没变，没必要生成seq
func (this *DB) getItemIndex(index *Index, item Item) ([]byte, error) {
	indexData, err := item.Index(index.config.Name)
	if err != nil {
		return nil, err
	}
	return getIndexDataPrefix(index.config.ID, indexData), nil
}

func (this *DB) getCurrentItemIndexes(table *Table, item Item) ItemIndexes {
	itemIndexes := ItemIndexes{}
	for indexName, index := range table.indexes {
		indexData, err := this.getItemIndex(index, item)
		if err == nil {
			itemIndexes[indexName] = indexData
		}

	}
	return itemIndexes
}

func (this *DB) Close() error {
	this.tableLock.Lock()
	defer this.tableLock.Unlock()
	if this.db == nil {
		return nil
	}
	this.seq.Release()
	for _, table := range this.tables {
		table.Close()
	}
	//  停止gc，sync
	this.cancel()
	this.wg.Wait()
	this.db.Close()
	this.db = nil
	return nil
}

/**
handle ErrConflict, ErrRetry.
*/
func (this *DB) Update(fn func(txn *badger.Txn) error, runBefore func()) error {
	var (
		err error
	)
	if runBefore != nil {
		runBefore()
	}
	err = this.db.Update(fn)
	if err == badger.ErrConflict || err == badger.ErrRetry {
		for {
			if runBefore != nil {
				runBefore()
			}
			err = this.db.Update(fn)
			if err != badger.ErrConflict && err != badger.ErrRetry {
				return err
			}
		}
	}
	return err
}

/**
handle ErrRetry, when gc cause vlog changed, we can assume no errors now
*/
func (this *DB) View(fn func(txn *badger.Txn) error, runBefore func()) error {
	var (
		err error
	)
	if runBefore != nil {
		runBefore()
	}
	err = this.db.View(fn)
	if err == badger.ErrRetry {
		for {
			if runBefore != nil {
				runBefore()
			}
			err = this.db.View(fn)
			if err != badger.ErrRetry {
				return err
			}
		}
	}
	return err
}

/**
can omit error now
*/
func (this *DB) NextID(seq *badger.Sequence) (uint64, error) {
	reply, err := seq.Next()
	if err == badger.ErrRetry || err == badger.ErrConflict {
		for {
			reply, err = seq.Next()
			if err != badger.ErrRetry && err != badger.ErrConflict {
				return reply, err
			}
		}
	}
	return reply, err
}

func (this *DB) gcLoop(freq time.Duration) {
	defer this.wg.Done()
	tk := time.NewTicker(freq)
	for {
		select {
		case <-tk.C:
			err := this.db.RunValueLogGC(0.5)
			if err != nil && err != badger.ErrNoRewrite {
				log.Println("gcLoop: ", err.Error())
			}
		case <-this.ctx.Done():
			return
		}
	}
}

func (this *DB) syncLoop(freq time.Duration) {
	defer this.wg.Done()
	tk := time.NewTicker(freq)
	for {
		select {
		case <-tk.C:
			err := this.db.Sync()
			if err != nil {
				log.Println("syncLoop: ", err.Error())
			}
		case <-this.ctx.Done():
			return
		}
	}
}

type IteratorOptions struct {
	TableName string
	IndexName string
	Reverse   bool
	Update    bool
	Begin     []interface{}
	End       []interface{}
	BeforeRun func()
	Callback  func(*badger.Item) (bool, error)
}

func (this *DB) EachItem(opt IteratorOptions) error {
	this.tableLock.RLock()
	defer this.tableLock.RUnlock()
	table, ok := this.tables[opt.TableName]
	if !ok {
		return ErrTableNotExists
	}
	index, ok := table.indexes[opt.IndexName]
	if !ok {
		return ErrIndexNotExists
	}
	begin := getIndexDataPrefix(index.config.ID, opt.Begin)
	end := getIndexDataPrefix(index.config.ID, append(opt.End, orderedcode.Infinity))
	trans := this.View
	if opt.Update {
		trans = this.Update
	}
	return trans(func(txn *badger.Txn) error {
		badgerOpt := badger.DefaultIteratorOptions
		badgerOpt.Reverse = opt.Reverse
		iter := txn.NewIterator(badgerOpt)
		defer iter.Close()
		if opt.Reverse {
			iter.Seek(end)
		} else {
			iter.Seek(begin)
		}
		for ; iter.Valid(); iter.Next() {
			item := iter.Item()
			if badgerOpt.Reverse {
				if bytes.Compare(item.Key(), begin) == -1 {
					return nil
				}
			} else {
				if bytes.Compare(item.Key(), end) == 1 {
					return nil
				}
			}
			var itemData *badger.Item
			err := item.Value(func(val []byte) (err error) {
				itemData, err = txn.Get(getItemDataKey(table.config.DataID, string(val)))
				return
			})
			if err != nil {
				return err
			}
			stop, err := opt.Callback(itemData)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
		}
		return nil
	}, opt.BeforeRun)
}

func (this *DB) EachIndex(opt IteratorOptions) error {
	this.tableLock.RLock()
	defer this.tableLock.RUnlock()
	table, ok := this.tables[opt.TableName]
	if !ok {
		return ErrTableNotExists
	}
	index, ok := table.indexes[opt.IndexName]
	if !ok {
		return ErrIndexNotExists
	}
	begin := getIndexDataPrefix(index.config.ID, opt.Begin)
	end := getIndexDataPrefix(index.config.ID, append(opt.End, orderedcode.Infinity))
	trans := this.View
	if opt.Update {
		trans = this.Update
	}
	return trans(func(txn *badger.Txn) error {
		badgerOpt := badger.DefaultIteratorOptions
		badgerOpt.Reverse = opt.Reverse
		iter := txn.NewIterator(badgerOpt)
		defer iter.Close()
		if opt.Reverse {
			iter.Seek(end)
		} else {
			iter.Seek(begin)
		}
		for ; iter.Valid(); iter.Next() {
			item := iter.Item()
			if badgerOpt.Reverse {
				if bytes.Compare(item.Key(), begin) == -1 {
					return nil
				}
			} else {
				if bytes.Compare(item.Key(), end) == 1 {
					return nil
				}
			}
			stop, err := opt.Callback(item)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
		}
		return nil
	}, opt.BeforeRun)
}
