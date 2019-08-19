package badgerquery

import (
	"github.com/dgraph-io/badger"
	"github.com/vmihailenco/msgpack"
)

func DecodeTableConfig(b []byte) (*TableConfig, error) {
	reply := new(TableConfig)
	err := msgpack.Unmarshal(b, reply)
	return reply, err
}

func EncodeTableConfig(t *TableConfig) []byte {
	b, _ := msgpack.Marshal(t)
	return b
}

type TableConfig struct {
	Name    string
	DataID  uint64
	IndexID uint64
	Deleted bool
}

type Table struct {
	config  *TableConfig
	seq     *badger.Sequence
	indexes map[string]*Index
	db      *DB
}

func (this *Table) Index(name string) *Index {
	return this.indexes[name]
}

func (this *Table) Close() error {
	for _, index := range this.indexes {
		index.Close()
	}
	return nil
}
