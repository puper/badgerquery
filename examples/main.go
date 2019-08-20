package main

import (
	"log"
	"runtime"
	"strconv"

	"github.com/vmihailenco/msgpack"

	"time"

	"github.com/dgraph-io/badger"
	"github.com/puper/badgerquery"
)

type Test struct {
	A int64
	B int64
}

func (this *Test) Key() string {
	return strconv.Itoa(int(this.A))
}

func (this *Test) Index(indexName string) ([]interface{}, error) {
	return []interface{}{this.B}, nil
}

func (this *Test) TableName() string {
	return "testTable"
}

func (this *Test) Marshal() []byte {
	data, _ := msgpack.Marshal(this)
	return data
}

func (this *Test) Unmarshal(data []byte) error {
	return msgpack.Unmarshal(data, this)
}

func main() {
	runtime.GOMAXPROCS(128)
	opt := badger.DefaultOptions("testDB")
	opt.SyncWrites = false
	db, _ := badgerquery.Open(opt, badgerquery.Options{
		GCFreq:   5 * time.Second,
		SyncFreq: 100 * time.Millisecond,
	})
	defer db.Close()
	db.CreateTable("testTable", map[string]*badgerquery.IndexConfig{
		"testIndex": &badgerquery.IndexConfig{},
	})
	/**
	start := time.Now()
	for i := 1000000; i < 2000000; i++ {
		err := db.SaveItem(&Test{int64(i), int64(i)})
		if err != nil {
			log.Println(1111111111, err)
		}
	}
	log.Println("cost", time.Now().Sub(start))
	*/
	item := &Test{}
	start := time.Now()
	for i := 2000000; i < 2000001; i++ {
		err := db.GetItem(strconv.Itoa(i), item)
		if err != nil {
			log.Println("get error: ", err)
		}
	}
	log.Println("getItem", time.Now().Sub(start))
	err := db.GetItem("5", item)
	log.Println(123333, err, item)
	start = time.Now()
	iterOpt := badgerquery.IteratorOptions{
		TableName: "testTable",
		IndexName: "testIndex",
		Reverse:   false,
		Begin:     []interface{}{int64(0)},
		End:       []interface{}{int64(1000000)},
		Callback: func(item *badger.Item) (bool, error) {
			item.Value(func(val []byte) error {
				item := new(Test)
				item.Unmarshal(val)
				//log.Println("item: ", err, item)
				return nil
			})
			return false, nil
		},
	}
	db.EachItem(iterOpt)

	log.Println(time.Now().Sub(start))

}
