package benchmark

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"math/rand"
	"time"
)

type SaddBenchmark struct {
	TestConfig
}

func NewSaddBenchmark(config *TestConfig) Runner {
	bm := &SaddBenchmark{}
	bm.HostPort = config.HostPort
	bm.ClientCount = config.ClientCount
	bm.Iterations = config.Iterations
	bm.ClientIterations = config.ClientIterations
	bm.Variant1 = config.Variant1
	bm.Variant2 = config.Variant2

	return bm
}

var _ Runner = (*SaddBenchmark)(nil)

func (sadd *SaddBenchmark) Launch() {
	clients := make([]*redis.Client, 0, sadd.ClientCount)

	for i := 0; i < sadd.ClientCount; i++ {
		client := redis.NewClient(&redis.Options{
			Addr: sadd.HostPort[i % len(sadd.HostPort)],
		})

		clients = append(clients, client)
	}

	testStartTime := time.Now()

	results := make(chan time.Duration)

	for _, c := range clients {
		go sadd.doSadd(c, results)
	}

	latencies := make(map[int]int)
	resultCount := new(int)
	*resultCount = 0

	tickQuitter := make(chan struct{})
	go throughputTicker(resultCount, tickQuitter)

	// Process results
	for r := range results {
		latencies[int(r.Milliseconds()) + 1]++

		*resultCount++
		if *resultCount == sadd.Iterations {
			break
		}
	}

	// Stop the ticker
	tickQuitter <- struct{}{}

	fmt.Println()
	printSummary(latencies)

	throughput := float64(sadd.Iterations) / time.Now().Sub(testStartTime).Seconds()

	fmt.Println()
	fmt.Printf("Clients:    %d\n", sadd.ClientCount)
	fmt.Printf("Operations: %d\n", *resultCount)
	fmt.Printf("Throughput: %0.2f ops/sec\n", throughput)
	fmt.Println()
}

func (sadd *SaddBenchmark) doSadd(client *redis.Client, results chan time.Duration) {
	randInt := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < sadd.ClientIterations; i++ {
		executionStartTime := time.Now()
		err := client.SAdd(fmt.Sprintf("mykey-%d", randInt.Intn(sadd.Variant1)), fmt.Sprintf("value-%d", rand.Intn(sadd.Variant2))).Err()
		if err != nil {
			panic(err)
		}

		latency := time.Now().Sub(executionStartTime)

		results <- latency
	}
}
