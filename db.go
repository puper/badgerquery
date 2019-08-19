package badgerquery

import (
	"bytes"

	"github.com/dgraph-io/badger"
)

type DB struct {
	tables map[string]*Table
	db     *badger.DB
	seq    *badger.Sequence
}

func (this *DB) loadMeta() error {
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

func (this *DB) CreateItem(item Item) error {
	err := this.Update(func(txn *badger.Txn) error {
		return this.createItem(txn, item)
	}, nil)
	return err
}

func (this *DB) UpdateItem(item Item) error {
	err := this.Update(func(txn *badger.Txn) error {
		return this.updateItem(txn, item)
	}, nil)
	return err
}

func (this *DB) Save(item Item) error {
	err := this.Update(func(txn *badger.Txn) error {
		table := this.tables[item.TableName()]
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

func (this *DB) DeleteItem(tableName string, itemKey string) error {
	err := this.Update(func(txn *badger.Txn) error {
		table := this.tables[tableName]
		return this.deleteItem(txn, table, itemKey)
	}, nil)
	return err
}

func (this *DB) GetItem(tableName string, item Item) error {
	err := this.View(func(txn *badger.Txn) error {
		table := this.tables[tableName]
		err := this.getItemData(txn, table.config.DataID, item.Key(), item)
		return err
	}, nil)
	return err
}

func (this *DB) HasItem(tableName string, itemKey string) (bool, error) {
	var (
		has bool
		err error
	)
	this.View(func(txn *badger.Txn) error {
		table := this.tables[tableName]
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
	table := this.tables[tableName]
	tableDataID := table.config.DataID
	tableIndexID := table.config.IndexID
	itemKey := item.Key()
	err = this.setItemData(txn, tableDataID, itemKey, item.Marshal())
	if err != nil {
		return err
	}
	itemIndexes := this.getCurrentItemIndexes(table, item)
	for _, indexData := range itemIndexes {
		err = this.addIndexData(txn, itemKey, indexData)
		if err != nil {
			return err
		}
	}
	err = this.setItemIndexes(txn, tableIndexID, itemKey, this.getCurrentItemIndexes(table, item))
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
	} else {
		return err
	}
	currentItemIndexes := this.getCurrentItemIndexes(table, item)
	err = this.setItemIndexes(txn, tableIndexID, itemKey, currentItemIndexes)
	if err != nil {
		return err
	}
	for indexName, currentIndexData := range currentItemIndexes {
		if oldIndexData, ok := oldItemIndexes[indexName]; !ok {
			this.addIndexData(txn, itemKey, currentIndexData)
		} else {
			delete(oldItemIndexes, indexName)
			if !bytes.Equal(oldIndexData, currentIndexData) {
				err = this.addIndexData(txn, itemKey, currentIndexData)
				if err != nil {
					return err
				}
				err = this.deleteIndexData(txn, oldIndexData)
				if err != nil && err != badger.ErrKeyNotFound {
					return err
				}
			}
		}
	}
	for _, oldIndexData := range oldItemIndexes {
		err = this.deleteIndexData(txn, oldIndexData)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
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

func (this *DB) getItemIndex(index *Index, item Item) ([]byte, error) {
	indexData, err := item.Index(index.config.Name)
	if err != nil {
		return nil, err
	}
	if index.config.Unique {
		return getIndexDataKey(index.config.ID, indexData, 0), nil
	} else {
		seq, err := this.NextID(index.seq)
		if err != nil {
			return nil, err
		}
		return getIndexDataKey(index.config.ID, indexData, seq), nil
	}
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

func (this *DB) xxx(item Item) error {
	err := this.Update(func(txn *badger.Txn) error {
		table := this.tables[item.TableName()]
		itemDataKey := getItemDataKey(table.config.DataID, item.Key())
		_, err := txn.Get(itemDataKey)
		if err != badger.ErrKeyNotFound {
			return err
		}
		itemIndexKey := getItemIndexKey(table.config.IndexID, item.Key())
		err = txn.Set(itemDataKey, item.Marshal())
		if err != nil {
			return err
		}
		itemIndexes := ItemIndexes{}
		for indexName, index := range table.indexes {
			indexData, err := item.Index(indexName)
			if err != nil {
				continue
			}
			seq, err := this.NextID(index.seq)
			if err != nil {
				return err
			}
			indexKey := getIndexDataKey(index.config.ID, indexData, seq)
			err = txn.Set(indexKey, []byte(item.Key()))
			if err != nil {
				return err
			}
			itemIndexes[indexName] = indexKey
		}
		if len(itemIndexes) > 0 {
			err := txn.Set(itemIndexKey, itemIndexes.Marshal())
			if err != nil {
				return err
			}
		}
		return nil
	}, nil)
	return err
}

func (this *DB) Close() error {
	this.seq.Release()
	for _, table := range this.tables {
		table.Close()
	}
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

func (this *DB) Test() {
	this.db.View(func(txn *badger.Txn) error {
		opt := badger.DefaultIteratorOptions
		opt.Prefix = getIndexDataKeyBase(this.tables["test"].indexes["index1"].config.ID)
		iter := txn.NewIterator(opt)
		defer iter.Close()
		for iter.Rewind(); iter.ValidForPrefix(opt.Prefix); iter.Next() {
			iter.Item().Value(func(val []byte) error {
				//log.Println(iter.Item().Key(), val)
				return nil
			})
		}
		return nil
	})
}
