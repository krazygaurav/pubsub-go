package main

import (
	"fmt"
	"math/rand"
	"time"

	"./pubsub"
)

// available topics
var availableTopics = map[string]string{
	"BTC": "BITCOIN",
	"ETH": "ETHEREUM",
	"DOT": "POLKADOT",
	"SOL": "SOLANA",
}

func pricePublisher(broker *pubsub.Broker)(){
	topicKeys := make([]string, 0, len(availableTopics))
	topicValues := make([]string, 0, len(availableTopics))

	for k, v := range availableTopics {
		topicKeys = append(topicKeys, k)
		topicValues = append(topicValues, v)
	}
	for {
		randValue := topicValues[rand.Intn(len(topicValues))] // all topic values.
		msg:= fmt.Sprintf("%f", rand.Float64())
		// fmt.Printf("Publishing %s to %s topic\n", msg, randKey)
		go broker.Publish(randValue, msg)
        // Uncomment if you want to broadcast to all topics.
		// go broker.Broadcast(msg, topicValues)
		r := rand.Intn(4)
		time.Sleep(time.Duration(r) * time.Second) //sleep for random secs.
	}
}



func main(){
    // construct new broker.
	broker := pubsub.NewBroker()

    // create new subscriber
	s1 := broker.AddSubscriber()
    // subscribe BTC and ETH to s1.
	broker.Subscribe(s1, availableTopics["BTC"])
	broker.Subscribe(s1, availableTopics["ETH"])

    // create new subscriber
	s2 := broker.AddSubscriber()
    // subscribe ETH and SOL to s2.
	broker.Subscribe(s2, availableTopics["ETH"])
	broker.Subscribe(s2, availableTopics["SOL"])

	go (func(){
		// sleep for 5 sec, and then subscribe for topic DOT for s2
		time.Sleep(3*time.Second)
		broker.Subscribe(s2, availableTopics["DOT"])
	})()

	go (func(){
		// s;eep for 5 sec, and then unsubscribe for topic SOL for s2
		time.Sleep(5*time.Second)
		broker.Unsubscribe(s2, availableTopics["SOL"])
		fmt.Printf("Total subscribers for topic ETH is %v\n", broker.GetSubscribers(availableTopics["ETH"]))
	})()


	go (func(){
		// s;eep for 5 sec, and then unsubscribe for topic SOL for s2
		time.Sleep(10*time.Second)
		broker.RemoveSubscriber(s2)
		fmt.Printf("Total subscribers for topic ETH is %v\n", broker.GetSubscribers(availableTopics["ETH"]))
	})()

    // Concurrently publish the values.
	go pricePublisher(broker)
    // Concurrently listens from s1.
	go s1.Listen()
    // Concurrently listens from s2.
	go s2.Listen()

	// to prevent terminate
	fmt.Scanln()
	fmt.Println("Done!")
}