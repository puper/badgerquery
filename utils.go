package badgerquery

import (
	"github.com/google/orderedcode"
)

var (
	metaSeqKey         = mustEncodeOrderedCode(nil, "meta", "seq")
	tableConfigKeyBase = mustEncodeOrderedCode(nil, "meta", "table", "config")
	indexConfigKeyBase = mustEncodeOrderedCode(nil, "meta", "table", "index", "config")
	indexSeqKeyBase    = mustEncodeOrderedCode(nil, "meta", "table", "index", "seq")
)

func mustEncodeOrderedCode(buf []byte, items ...interface{}) []byte {
	reply, err := orderedcode.Append(buf, items...)
	if err != nil {
		panic("MustEncodeOrderedCode: " + err.Error())
	}
	return reply
}

func DecodeIndexData(buf []byte, items ...interface{}) error {
	var (
		indexID uint64
	)
	remain, err := orderedcode.Parse(string(buf), &indexID)
	if err != nil {
		return err
	}
	_, err = orderedcode.Parse(remain, items)
	return err
}

func getMetaSeqKey() []byte {
	return metaSeqKey
}

func getTableConfigKey(tableName string) []byte {
	return mustEncodeOrderedCode(tableConfigKeyBase, tableName)
}

func getIndexConfigKey(tableName string, indexName string) []byte {
	return mustEncodeOrderedCode(indexConfigKeyBase, tableName, indexName)
}

func getIndexSeqKey(tableName string, indexName string) []byte {
	return mustEncodeOrderedCode(indexSeqKeyBase, tableName, indexName)
}

func getItemDataKeyBase(tableDataID uint64) []byte {
	return mustEncodeOrderedCode(nil, tableDataID)
}

func getItemDataKey(tableDataID uint64, itemKey interface{}) []byte {
	return mustEncodeOrderedCode(nil, tableDataID, itemKey)
}

func getItemIndexKeyBase(tableIndexID uint64) []byte {
	return mustEncodeOrderedCode(nil, tableIndexID)
}

func getItemIndexKey(tableIndexID uint64, itemKey interface{}) []byte {
	return mustEncodeOrderedCode(nil, tableIndexID, itemKey)
}

func getIndexDataKeyBase(indexID uint64) []byte {
	return mustEncodeOrderedCode(nil, indexID)
}

func getIndexDataKey(indexID uint64, indexData []interface{}, seq uint64) []byte {
	reply := mustEncodeOrderedCode(nil, indexID)
	reply = mustEncodeOrderedCode(reply, indexData...)
	reply = mustEncodeOrderedCode(reply, seq)
	return reply
}

func getIndexDataPrefix(indexID uint64, indexData []interface{}) []byte {
	reply := mustEncodeOrderedCode(nil, indexID)
	reply = mustEncodeOrderedCode(reply, indexData...)
	return reply
}
