package badgerquery

import (
	"github.com/1lann/msgpack"
	"github.com/dgraph-io/badger"
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
	Unique    bool
	Deleted   bool
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
