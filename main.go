package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"runtime"
	"sync"
	"time"

	metrics "github.com/rcrowley/go-metrics"
)

var concurrency *int = flag.Int("concurrency", 4, `how many workers to use while writing to the db file`)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(8)

	//bolt_test(*concurrency)

	//esdb_test(*concurrency)

	//kafka_test(*concurrency)

	//rocks_test(*concurrency)

	forestdb_test(*concurrency)

	wg := new(sync.WaitGroup)
	wg.Add(1)

	//startapi()

	wg.Wait()

}

func logMetrics(writeMeter string, concurrency int) {
	ticker := time.NewTicker(time.Second * 10)
	stime := time.Now().Unix()
	fmt.Println("concurrency, seconds_run, puts_sec, event_count")
	for now := range ticker.C {

		wrtsize := metrics.DefaultRegistry.Get(writeMeter).(metrics.Meter)
		runtime := now.Unix() - stime

		fmt.Printf("       %d, %d, %f, %d\n",
			concurrency, runtime, wrtsize.RateMean(), wrtsize.Count())
	}
}

type testentity struct {
	Key        string
	FirstName  string
	LastName   string
	Address    string
	CreateTime int64
}

func createdata(key string) []byte {
	data := &testentity{
		Key:        key,
		FirstName:  "Eric",
		LastName:   "Foo",
		Address:    "1234 loadtest street, MainLandCity, AZ 97123",
		CreateTime: time.Now().Unix(),
	}

	if bytes, err := json.Marshal(data); err != nil {
		log.Printf("createdata error: %v", err)
		return nil
	} else {
		return bytes
	}
}

func startapi() {
	http.HandleFunc("/metrics", func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Content-Type", "application/json")

		json.NewEncoder(rw).Encode(metrics.DefaultRegistry)
	})

	bind := ":5103"
	conn, err := net.Listen("tcp", bind)
	if err != nil {
		log.Printf("[metrics] Error listening on %s: %v", bind, err)
		return
	}

	log.Printf("[metrics] Metrics visible at: http://localhost%s", bind)
	go func() {
		if err := http.Serve(conn, http.DefaultServeMux); err != nil {
			log.Printf("[metrics] Error from http.Serve(...): %v", err)
		}
		conn.Close()
	}()
}

/*


type lqlhandler struct {
	lqlhandlers map[string]http.Handler

	mu sync.Mutex
}

func (lh *lqlhandler) addHandler(aid string, handler http.Handler) {
	lh.lqlhandlers[aid] = handler
}

func (lh *lqlhandler) setBaseHandler() {
	http.HandleFunc("/lql/", func(resp http.ResponseWriter, req *http.Request) {
		lh.mu.Lock()
		defer lh.mu.Unlock()

		aidstr := req.URL.Path[1:2]
		if handler, ok := lqlhandler.lqlhandlers[aidstr]; ok {
			return handler(resp, req)
		}
		return http.NotFound(resp, req)
	})
}

*/
