package benchmark

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"math/rand"
	"time"
)

type DelBenchmark struct {
	config *TestConfig
}

func NewDelBenchmark(config *TestConfig) *DelBenchmark {
	return &DelBenchmark{config}
}

var _ Runner = (*DelBenchmark)(nil)


func (delBenchmark *DelBenchmark) Launch() {
	clients := make([]*redis.Client, 0, delBenchmark.config.ClientCount)
	clients = setUpClients(delBenchmark, clients)
	testStartTime := time.Now()
	results := make(chan time.Duration)

	for _, c := range clients {
		go delBenchmark.doDel(c, results)
	}

	latencies := make(map[int]int)
	resultCount := new(int)
	*resultCount = 0

	tickQuitter := make(chan struct{})
	go throughputTicker(resultCount, tickQuitter)

	delBenchmark.processResults(results, latencies, resultCount)

	// Stop the ticker
	tickQuitter <- struct{}{}

	delBenchmark.printResults(latencies, testStartTime, resultCount)
}

func (delBenchmark *DelBenchmark) doDel(client *redis.Client, results chan time.Duration) {
	randInt := rand.New(rand.NewSource(time.Now().UnixNano()))

	var members = make([]interface{}, delBenchmark.config.Variant2)
	for j := 0; j < delBenchmark.config.Variant2; j++ {
		members[j] = fmt.Sprintf("myValue-%d",j)
	}

	for i := 0; i < delBenchmark.config.ClientIterations; i++ {

		key := fmt.Sprintf("mykey-%d", randInt.Intn(delBenchmark.config.Variant1))

		err := client.SAdd(key, members...).Err()
		if err != nil && !delBenchmark.config.IgnoreErrors {
			panic(err)
		}

		executionStartTime := time.Now()
		err = client.Del(key).Err()
		if err != nil && !delBenchmark.config.IgnoreErrors {
			panic(err)
		}

		latency := time.Now().Sub(executionStartTime)

		results <- latency
	}
}

func (delBenchmark *DelBenchmark) processResults(
	results chan time.Duration,
	latencies map[int]int,
	resultCount *int)  {

	for result := range results {
		latencies[int(result.Milliseconds())+1]++

		*resultCount++
		if *resultCount == delBenchmark.config.Iterations {
			break
		}
	}
}

func (delBenchmark *DelBenchmark)printResults(
	latencies map[int]int,
	testStartTime time.Time,
	resultCount *int) {
	fmt.Println()
	printSummary(latencies)

	throughput :=
		float64(delBenchmark.config.Iterations) / time.Now().Sub(testStartTime).Seconds()

	fmt.Println()
	fmt.Printf("Clients:    %d\n", delBenchmark.config.ClientCount)
	fmt.Printf("Operations: %d\n", *resultCount)
	fmt.Printf("Throughput: %0.2f ops/sec\n", throughput)
	fmt.Println()
}


func setUpClients(delBenchmark *DelBenchmark, clients []*redis.Client) []*redis.Client {
	for i := 0; i < delBenchmark.config.ClientCount; i++ {
		client := redis.NewClient(&redis.Options{
			Addr: delBenchmark.config.HostPort[i%len(delBenchmark.config.HostPort)],
		})

		clients = append(clients, client)
	}
	return clients
}
