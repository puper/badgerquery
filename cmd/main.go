package main

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"sync"

	"time"

	"github.com/1lann/msgpack"
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
	return "test"
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
	opt := badger.DefaultOptions("test1")
	opt.SyncWrites = false
	db, err := badgerquery.Open(opt)
	defer db.Close()
	log.Println(db, err)
	err = db.CreateTable("test", map[string]*badgerquery.IndexConfig{
		"index1": &badgerquery.IndexConfig{
			Unique: true,
		},
	})
	var wg1 sync.WaitGroup
	wg1.Add(5)
	go func() {
		defer wg1.Done()
		create(db, 0, 100000)
	}()
	go func() {
		defer wg1.Done()
		create(db, 100000, 200000)
	}()
	go func() {
		defer wg1.Done()
		create(db, 200000, 300000)
	}()
	go func() {
		defer wg1.Done()
		create(db, 300000, 400000)
	}()
	go func() {
		defer wg1.Done()
		create(db, 400000, 500000)
	}()
	wg1.Wait()
	/**
	start = time.Now()
	for i := 0; i < 10000; i++ {

		err = db.DeleteItem("test", strconv.Itoa(i))
	}
	fmt.Printf("%v\n", time.Now().Sub(start))
	*/
	item := &Test{
		A: 999,
	}
	start := time.Now()
	for i := 0; i < 100000; i++ {

		err = db.GetItem("test", item)
	}
	fmt.Printf("%v\n", time.Now().Sub(start))

	start = time.Now()
	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		db.Test()
	}()
	go func() {
		defer wg.Done()
		db.Test()
	}()
	go func() {
		defer wg.Done()
		db.Test()
	}()
	go func() {
		defer wg.Done()
		db.Test()
	}()
	go func() {
		defer wg.Done()
		db.Test()
	}()
	wg.Wait()
	fmt.Printf("%v\n", time.Now().Sub(start))
	log.Println(err, item)

}

func create(db *badgerquery.DB, min, max int) {
	start := time.Now()
	for i := min; i < max; i++ {

		db.CreateItem(&Test{
			A: int64(i),
			B: int64(i),
		})
	}
	fmt.Printf("%v, %v, %v\n", min, max, time.Now().Sub(start))
}
