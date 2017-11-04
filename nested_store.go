package main

import (
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"io/ioutil"
	"encoding/json"
	"strconv"
	"os"
	"time"
	"math/rand"
	"bytes"
)

func HandleErr(err error) {
	if err != nil {
		panic(err)
	}
}

func CheckBucket(b *bolt.Bucket) {
	if b == nil {
		panic("no bucket was found")
	}
}

type Nested map[string]map[string]map[string]int

func GetNestedData() Nested {
	dataBin, err := ioutil.ReadFile("bolt_performance/store/data.json")
	HandleErr(err)

	data := Nested{}

	err = json.Unmarshal(dataBin, &data)
	HandleErr(err)

	return data
}

func PutNestedBatchData(db *bolt.DB) {
	dat := GetNestedData()

	err := db.Batch(func(tx *bolt.Tx) error {
		b1, err := tx.CreateBucketIfNotExists([]byte("FirstLevel"))
		HandleErr(err)

		for k1, v1 := range dat {
			b2, err := b1.CreateBucketIfNotExists([]byte(k1))
			HandleErr(err)

			for k2, v2 := range v1 {
				b3, err := b2.CreateBucketIfNotExists([]byte(k2))
				HandleErr(err)

				for k3, v3 := range v2 {
					b3.Put([]byte(k3), []byte(strconv.Itoa(v3)))
				}
			}
		}

		return nil
	})

	if err != nil {
		log.Panic(err)
	}
}

func UpdateSingleRecord(db *bolt.DB, data *map[string]map[string]map[string]int) {
	i := 0
	for k1, v1 := range *data {
		for k2, v2 := range v1 {
			for k3, v3 := range v2 {
				i += 1
				err := db.Update(func(tx *bolt.Tx) error {
					b1 := tx.Bucket([]byte("FirstLevel"))
					if b1 == nil {
						return fmt.Errorf("no bucket FirstLevel found")
					}

					b2 := b1.Bucket([]byte(k1))
					if b2 == nil {
						return fmt.Errorf("no bucket %s found", k1)
					}

					b3 := b2.Bucket([]byte(k2))
					if b3 == nil {
						return fmt.Errorf("no bucket %s found", k2)
					}

					b3.Put([]byte(k3), []byte(strconv.Itoa(v3)))

					fmt.Printf("\rDone %d", i)
					return nil
				})
				HandleErr(err)
			}
		}
	}
}

func UpdateBatchRecord(db *bolt.DB, data *map[string]map[string]map[string]int) {
	err := db.Batch(func(tx *bolt.Tx) error {
		b1 := tx.Bucket([]byte("FirstLevel"))
		if b1 == nil {
			return fmt.Errorf("no FirstLevel bucket found")
		}

		for k1, v1 := range *data {
			b2 := b1.Bucket([]byte(k1))
			if b2 == nil {
				return fmt.Errorf("no bucket %s found", k1)
			}

			for k2, v2 := range v1 {
				b3 := b2.Bucket([]byte(k2))
				if b3 == nil {
					return fmt.Errorf("no bucket %s found", k2)
				}

				for k3, v3 := range v2 {
					b3.Put([]byte(k3), []byte(strconv.Itoa(v3)))
				}
			}
		}

		return nil
	})
	HandleErr(err)
}

func ReadSingleData(db *bolt.DB, data *map[string]map[string]map[string]int) {
	i := 0
	for k1, v1 := range *data {
		for k2, v2 := range v1 {
			for k3, _ := range v2 {
				i += 1
				err := db.View(func(tx *bolt.Tx) error {
					b1 := tx.Bucket([]byte("FirstLevel"))
					if b1 == nil {
						return fmt.Errorf("no bucket FirstLevel found")
					}

					b2 := b1.Bucket([]byte(k1))
					if b2 == nil {
						return fmt.Errorf("no bucket %s found", k1)
					}

					b3 := b2.Bucket([]byte(k2))
					if b3 == nil {
						return fmt.Errorf("no bucket %s found", k2)
					}

					_ = b3.Get([]byte(k3))

					fmt.Printf("\rDone Reading %d", i)
					return nil
				})
				HandleErr(err)
			}
		}
	}
	fmt.Printf("\r")
}

func ReadBatchRecord(db *bolt.DB, data *map[string]map[string]map[string]int) {
	err := db.Batch(func(tx *bolt.Tx) error {
		b1 := tx.Bucket([]byte("FirstLevel"))
		if b1 == nil {
			return fmt.Errorf("no FirstLevel bucket found")
		}

		for k1, v1 := range *data {
			b2 := b1.Bucket([]byte(k1))
			if b2 == nil {
				return fmt.Errorf("no bucket %s found", k1)
			}

			for k2, v2 := range v1 {
				b3 := b2.Bucket([]byte(k2))
				if b3 == nil {
					return fmt.Errorf("no bucket %s found", k2)
				}

				for k3, _ := range v2 {
					_ = b3.Get([]byte(k3))
				}
			}
		}

		return nil
	})
	HandleErr(err)
}

func DeleteAllData(db *bolt.DB) {
	var buffer bytes.Buffer

	for i := 0; i < 100; i++ {
		buffer.WriteString("key")
		buffer.WriteString(strconv.Itoa(i))
		firstKey := buffer.Bytes()
		buffer.Reset()

		err := db.Update(func(tx *bolt.Tx) error {
			b1 := tx.Bucket([]byte("FirstLevel"))
			if b1 == nil {
				return fmt.Errorf("no bucket FirstLevel found")
			}

			err := b1.DeleteBucket(firstKey)
			HandleErr(err)
			return nil
		})
		HandleErr(err)

	}
}

func GetRandomNestedDictionary() *map[string]map[string]map[string]int {
	var newData map[string]map[string]map[string]int
	newData = make(map[string]map[string]map[string]int)

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	var buffer bytes.Buffer

	for i := 0; i < 100; i++ {
		buffer.WriteString("key")
		buffer.WriteString(strconv.Itoa(i))
		firstKey := buffer.String()
		buffer.Reset()

		for j := 0; j < 100; j++ {
			buffer.WriteString("key")
			buffer.WriteString(strconv.Itoa(j))
			secondKey := buffer.String()
			buffer.Reset()

			for k := 0; k < 100; k++ {
				buffer.WriteString("key")
				buffer.WriteString(strconv.Itoa(k))
				thirdKey := buffer.String()
				buffer.Reset()

				if newData[firstKey] == nil {
					newData[firstKey] = make(map[string]map[string]int)
				}

				if newData[firstKey][secondKey] == nil {
					newData[firstKey][secondKey] = make(map[string]int)
				}

				newData[firstKey][secondKey][thirdKey] = r1.Intn(1000) * (i + 1) * (j + 2) * (k + 3)
			}
		}
	}
	return &newData
}

func main () {
	dbPath := "bolt_performance/store/nested.db"
	_ = os.Remove(dbPath)

	db, err := bolt.Open(dbPath, 0600, nil)
	HandleErr(err)

	defer db.Close()

	createStartTime := time.Now()
	PutNestedBatchData(db)
	createEndTime := time.Now()
	totalCreateTime := createEndTime.Sub(createStartTime)
	fmt.Println("Total Batch Create Time: ", totalCreateTime)

	randData1 := GetRandomNestedDictionary()
	//fmt.Println("Generated First nested random data")
	randData2 := GetRandomNestedDictionary()
	//fmt.Println("Generated Second nested random data")

	updateSingleStartTime := time.Now()
	UpdateSingleRecord(db, randData1)
	updateSingleEndTime := time.Now()
	totalSingleUpdateTime := updateSingleEndTime.Sub(updateSingleStartTime)
	fmt.Println("Total Single Update Time: ", totalSingleUpdateTime)

	updateBatchStartTime := time.Now()
	UpdateBatchRecord(db, randData2)
	updateBatchEndTime := time.Now()
	totalUpdateBatchTime := updateBatchEndTime.Sub(updateBatchStartTime)
	fmt.Println("Total Batch Update Time: ", totalUpdateBatchTime)

	readSingleStartTime := time.Now()
	ReadSingleData(db, randData2)
	readSingleEndTime := time.Now()
	totalSingleReadTime := readSingleEndTime.Sub(readSingleStartTime)
	fmt.Println("Total Single Read Time: ", totalSingleReadTime)

	readBatchStartTime := time.Now()
	ReadBatchRecord(db, randData2)
	readBatchEndTime := time.Now()
	totalBatchReadTime := readBatchEndTime.Sub(readBatchStartTime)
	fmt.Println("Total Batch Read Time: ", totalBatchReadTime)

	deleteSingleStartTime := time.Now()
	DeleteAllData(db)
	deleteSingleEndTime := time.Now()
	totalSingleDeleteTime := deleteSingleEndTime.Sub(deleteSingleStartTime)
	fmt.Println("Total Delete Time: ", totalSingleDeleteTime)
}
