package main

import (
	"fmt"
	"sync"
	"time"

	forestdb "github.com/couchbaselabs/goforestdb"
	"github.com/rcrowley/go-metrics"
)

/*

to install forest db

mkdir ~/Dropbox/go/root/src/github.com/couchbaselabs/
cd  ~/Dropbox/go/root/src/github.com/couchbaselabs/
git clone https://github.com/couchbaselabs/forestdb.git
cd forestdb
mkdir build
sudo apt-get install libsnappy-dev
sudo apt-get install cmake
cmake ../
make all
make install
sudo ldconfig # update lib cache

go get -u -v -t github.com/couchbaselabs/goforestdb

*/

const forestdbMeter = "forestdb.msg.meter"

func forestdb_test(concurrency int) {

	// Open a database
	db, err := forestdb.Open("test", nil)
	if err != nil {
		panic(err)
	}
	// Close it properly when we're done
	defer db.Close()

	//Start Writers
	wg := new(sync.WaitGroup)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go forestdb_writer(i, db, wg)
		time.Sleep(100 * time.Millisecond)
	}

	go logMetrics(forestdbMeter, concurrency)

	wg.Wait()
}

func forestdb_writer(id int, db *forestdb.File, wg *sync.WaitGroup) {
	defer wg.Done()
	start_time := time.Now().Unix()

	kvstore, err := db.OpenKVStoreDefault(forestdb.DefaultKVStoreConfig())
	if err != nil {
		panic(err)
	}
	defer kvstore.Close()

	gen := metrics.NewMeter()
	metrics.GetOrRegister(forestdbMeter, gen)
	for i1 := 1; i1 < 10000; i1++ {
		if time.Now().Unix() > start_time+120 {
			break
		}

		for i := 1; i < 10000; i++ { // Create 10 messages with in this transition.
			keystr := fmt.Sprintf("%d-%d-%d", i1, i, i)
			key := []byte(keystr)

			// Store the document
			kvstore.SetKV(key, createdata(keystr))

			gen.Mark(1)
		}
	}
}
