package main

import (
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"io/ioutil"
	"encoding/json"
	"time"
	"os"
)

func HandleErr(err error) {
	if err != nil {
		panic(err)
	}
}

type Flat map[string]*json.RawMessage

func GetData() Flat {
	dataBin, err := ioutil.ReadFile("bolt_performance/store/data.json")
	HandleErr(err)

	data := Flat{}

	err = json.Unmarshal(dataBin, &data)
	HandleErr(err)

	return data
}


func PutData(db *bolt.DB) {
	dat := GetData()

	err := db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("FirstLevel"))

		HandleErr(err)

		for k, v := range dat {
			vv, err := v.MarshalJSON()
			HandleErr(err)
			b.Put([]byte(k), vv)
		}

		return nil
	})

	if err != nil {
		log.Panic(err)
	}
}

func ReadOneData(db *bolt.DB) {
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("FirstLevel"))

		if b == nil {
			log.Panic("no bucket found")
		}

		val := b.Get([]byte("key1"))
		fmt.Println(string(val))

		return nil
	})
	HandleErr(err)
}

func main () {
	dbPath := "bolt_performance/store/flat.db"
	_ = os.Remove(dbPath)

	db, err := bolt.Open(dbPath, 0600, nil)
	HandleErr(err)

	defer db.Close()

	createStartTime := time.Now()
	PutData(db)
	createEndTime := time.Now()
	totalCreateTime := createEndTime.Sub(createStartTime)

	fmt.Println(totalCreateTime)
}
