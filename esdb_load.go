package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/customerio/esdb"
	"github.com/rcrowley/go-metrics"
)

var esdb_file *string = flag.String("esdb_file", "loadtest.esdb", `The fullpath to use for the esdb file`)

const esdb_msg_meter = "esdb.msg.meter"

func esdb_test(concurrency int) {

	//Start Writers
	wg := new(sync.WaitGroup)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go esdb_writer(i, wg)
	}

	go logMetrics(esdb_msg_meter, concurrency)

	wg.Wait()
	//fi, err := os.Stat(*esdb_file)
	//if err != nil {
	//	log.Printf("error dbsize: %v", err)
	//}
	//log.Printf("Final DB size is : %vmb", fi.Size()/1000000)
}

func esdb_writer(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	start_time := time.Now().Unix()

	gen := metrics.NewMeter()
	metrics.GetOrRegister(esdb_msg_meter, gen)
	for i1 := 1; i1 < 10000; i1++ {
		if time.Now().Unix() > start_time+120 {
			break
		}

		blockfile := fmt.Sprintf("%d.%d.%s", id, i1, *esdb_file)
		// In case we've already created the file.
		os.Remove(blockfile)
		writer, err := esdb.New(blockfile)
		if err != nil {
			panic(err)
		}
		for i := 1; i < 10000; i++ { // Create 10 messages with in this transition.
			gen.Mark(1)
			keystr := fmt.Sprintf("%d-%d-%d", i1, i, i)
			key := []byte(keystr)

			value := createdata(keystr)
			writer.Add(
				key,   // space the event will be stored under.
				value, // value can be any binary data.
				int((time.Now().UnixNano()>>32)<<32), // all events will be stored sorted by this value. // just the low order bits
				"", // grouping. "" here means no grouping, store sequentially by timestamp.
				map[string]string{
					"type": "email", // We'll define one secondary index on event type.
				},
			)
			gen.Mark(1)
		}
		err = writer.Write()
		if err != nil {
			panic(err)
		}
	}
}
