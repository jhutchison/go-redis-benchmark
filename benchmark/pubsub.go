package benchmark

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"strconv"
	"time"
)

type PubSubBenchmark struct {
	config *TestConfig
}

type Subscriber struct {
	id     int
	client *redis.Client
	pubsub *redis.PubSub
}

const (
	DONE = "DONE"
)

var _ Runner = (*PubSubBenchmark)(nil)

func NewPubSubBenchmark(config *TestConfig) *PubSubBenchmark {
	return &PubSubBenchmark{config}
}

func (bm *PubSubBenchmark) Launch() {
	channelName := "benchmarkChannel"

	subscribers := make([]*Subscriber, 0, bm.config.ClientCount)

	for i := 0; i < bm.config.ClientCount; i++ {
		// Establish connection
		client := redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		})

		pubsub := client.Subscribe(channelName)

		err := waitForSubscription(pubsub)
		if err != nil {
			panic(err)
		}

		subscription := &Subscriber{
			id:     i,
			client: client,
			pubsub: pubsub,
		}
		subscribers = append(subscribers, subscription)
	}

	fmt.Printf("Subscribed %d clients\n", bm.config.ClientCount)

	publisher := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	go bm.publishMessages(publisher, channelName)

	results := make(chan int)
	for _, s := range subscribers {
		go receiveMessages(s, results)
	}

	fmt.Println("Waiting for results")
	latencies := make(map[int]int)
	clientsDone := 0
	messagesReceived := 0
	for r := range results {
		if r < 0 {
			clientsDone++
			if clientsDone == bm.config.ClientCount {
				break
			}
			continue
		}

		latencies[r]++
		messagesReceived++
	}

	for _, s := range subscribers {
		_ = s.pubsub.Unsubscribe(channelName)
	}

	fmt.Println()
	fmt.Printf("Clients: %d\n", bm.config.ClientCount)
	fmt.Printf("Total messages received: %d\n\n", messagesReceived)
	printSummary(latencies)
}

func receiveMessages(subscriber *Subscriber, results chan int) {
	for msg := range subscriber.pubsub.Channel() {
		if msg.Payload == DONE {
			break
		}

		messageReceived := time.Now().UnixNano()

		messageCreated, err := strconv.Atoi(msg.Payload)
		if err != nil {
			panic(err)
		}

		latency := int(float64((messageReceived-int64(messageCreated))/1e6) + 1)
		results <- latency
	}

	// Indicate we're done receiving messages
	results <- -1
}

func (bm *PubSubBenchmark) publishMessages(publisher *redis.Client, channelName string) {
	for i := 0; i < bm.config.ClientIterations; i++ {
		startTime := time.Now()
		message := fmt.Sprintf("%d", startTime.UnixNano())
		err := publisher.Publish(channelName, message).Err()
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Millisecond * 20)
	}

	err := publisher.Publish(channelName, DONE).Err()
	if err != nil {
		panic(err)
	}
}

func waitForSubscription(pubSub *redis.PubSub) error {
	msgi, err := pubSub.ReceiveTimeout(time.Second * 2)
	if err != nil {
		return err
	}

	switch msg := msgi.(type) {
	case *redis.Subscription:
		return nil
	default:
		panic(fmt.Sprintf("Unexpected message waiting for subscription: %v", msg))
	}
}
