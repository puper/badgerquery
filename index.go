package badgerquery

import (
	"github.com/dgraph-io/badger"
	"github.com/vmihailenco/msgpack"
)

func DecodeIndexConfig(b []byte) (*IndexConfig, error) {
	reply := new(IndexConfig)
	err := msgpack.Unmarshal(b, reply)
	return reply, err
}

func EncodeIndexConfig(t *IndexConfig) []byte {
	b, _ := msgpack.Marshal(t)
	return b
}

type IndexConfig struct {
	TableName string
	ID        uint64
	Name      string
	Unique    bool // not used now
	Deleted   bool // not used now
}

type Index struct {
	config *IndexConfig
	seq    *badger.Sequence
	db     *DB
}

func (this *Index) Config() *IndexConfig {
	return this.config
}

func (this *Index) Close() error {
	return this.seq.Release()
}
