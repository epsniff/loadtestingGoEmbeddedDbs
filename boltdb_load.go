package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	metrics "github.com/rcrowley/go-metrics"
)

var boltdb_file *string = flag.String("bolt_file", "loadtest.boltdb", `The fullpath to use for the boltdb file`)

const bolt_test_bucket = "load_test_bucket"
const bolt_msg_meter = "bolt.msg.meter"

func bolt_test(concurrency int) {
	os.Remove(*boltdb_file)
	db, err := bolt.Open(*boltdb_file, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := db.Close()
		if err != nil {
			log.Printf("error closing db: %v", err)
		}
	}()

	//Start Writers
	wg := new(sync.WaitGroup)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go bolt_writer(i, wg, db)
	}

	go logMetrics(bolt_msg_meter, concurrency)

	wg.Wait()

	fi, err := os.Stat(*boltdb_file)
	if err != nil {
		log.Printf("error dbsize: %v", err)
	}
	log.Printf("Final DB size is : %vmb", fi.Size()/1000000)
}

func bolt_writer(id int, wg *sync.WaitGroup, db *bolt.DB) {
	defer wg.Done()
	start_time := time.Now().Unix()

	gen := metrics.NewMeter()
	metrics.GetOrRegister(bolt_msg_meter, gen)

	//We are going to flush our transactions every 10k messages, with experiments this seems to be the optimal size for this load test.
	for i1 := 1; i1 < 10000; i1++ {
		if time.Now().Unix() > start_time+120 {
			break
		}

		err := db.Update(func(tx *bolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte(fmt.Sprintf("%s-%d", bolt_test_bucket, id))) // Create one bucket per worker...
			if err != nil {
				log.Printf("error: %v", err)
				return err
			}

			for i2 := 1; i2 < 10000; i2++ { // Create 10 messages with in this transition.
				gen.Mark(1)
				keystr := fmt.Sprintf("%d-%d-%d", id, i1, i2)
				key := []byte(keystr)

				value := createdata(keystr)

				err = bucket.Put(key, value)
				if err != nil {
					log.Printf("error: %v", err)
				}
			}

			return nil
		})
		if err != nil {
			log.Printf("error transaction: %v", err)
		}
	}

	log.Printf("load worker done")

}
