package badgerquery

import (
	"github.com/vmihailenco/msgpack"
)

type Item interface {
	Key() string
	TableName() string
	Index(indexName string) ([]interface{}, error)
	Marshal() []byte
	Unmarshal([]byte) error
}

// index name = > index key
type ItemIndexes map[string][]byte

func (this *ItemIndexes) Marshal() []byte {
	data, _ := msgpack.Marshal(this)
	return data
}

func (this *ItemIndexes) Unmarshal(data []byte) error {
	return msgpack.Unmarshal(data, this)
}
