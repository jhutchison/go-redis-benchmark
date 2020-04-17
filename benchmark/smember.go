package benchmark

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"math/rand"
	"time"
)

type SmembersBenchmark struct {
	config *TestConfig
}

func NewSmembersBenchmark(config *TestConfig) Runner {
	return &SmembersBenchmark{config}
}

var _ Runner = (*SmembersBenchmark)(nil)

func (smembers *SmembersBenchmark) Launch() {
	clients := make([]*redis.Client, 0, smembers.config.ClientCount)

	for i := 0; i < smembers.config.ClientCount; i++ {
		client := redis.NewClient(&redis.Options{
			Addr: smembers.config.HostPort[i % len(smembers.config.HostPort)],
		})

		clients = append(clients, client)
	}

	smembers.setupSets(clients[0])

	testStartTime := time.Now()

	results := make(chan time.Duration)

	for _, c := range clients {
		go smembers.doSmembers(c, results)
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
		if *resultCount == smembers.config.Iterations {
			break
		}
	}

	// Stop the ticker
	tickQuitter <- struct{}{}

	fmt.Println()
	printSummary(latencies)

	throughput := float64(smembers.config.Iterations) / time.Now().Sub(testStartTime).Seconds()

	fmt.Println()
	fmt.Printf("Clients:    %d\n", smembers.config.ClientCount)
	fmt.Printf("Operations: %d\n", *resultCount)
	fmt.Printf("Throughput: %0.2f ops/sec\n", throughput)
	fmt.Println()
}

func (smembers *SmembersBenchmark) setupSets(client *redis.Client) {
	for i := 0; i < smembers.config.Variant1; i++ {
		key := fmt.Sprintf("mykey-%d", i)
		client.Del(key)
		for j := 0; j < smembers.config.Variant2; j++ {
			member := fmt.Sprintf("value-%d", j)
			err := client.SAdd(key, member).Err()
			if err != nil && !smembers.config.IgnoreErrors {
				panic(err)
			}
		}
	}
}

func (smembers *SmembersBenchmark) doSmembers(client *redis.Client, results chan time.Duration) {
	randInt := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < smembers.config.ClientIterations; i++ {
		executionStartTime := time.Now()

		key := fmt.Sprintf("mykey-%d", randInt.Intn(smembers.config.Variant1))

		 smembersResults := client.SMembers(key)
		 err := smembersResults.Err()

		//fmt.Printf("members: %s \n" , smembersResults.String())
		if err != nil && !smembers.config.IgnoreErrors {
			panic(err)
		}

		latency := time.Now().Sub(executionStartTime)
		results <- latency
	}
}
