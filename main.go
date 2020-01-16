package main

import (
	"fmt"
	"sync"
	"time"
)

/*
import (
	"coda/producer"
	"coda/consumer"
	"coda/broker"
)

func main() {

	producer.ProducerMain()
	consumer.ConsumerMain()
	broker.BrokerMain()

}

*/

/*

var mutexTopic []sync.Mutex
var mutexTopicIndx map[string]int
var topicCounter int
var m sync.Mutex

var c int

func increaser() {

	i := mutexTopicIndx["pippo"]

	for {

		mutexTopic[i].Lock()
		c = c + 1
		time.Sleep(1*time.Second)
		fmt.Printf("Increased c: %d\n", c)
		mutexTopic[i].Unlock()

	}


}


func main() {

	//adding topoc
	mutexTopicIndx = make(map[string]int)
	c = 0
	m.Lock()
	topicCounter = 0
	mutexTopic = append(mutexTopic, sync.Mutex{})
	mutexTopicIndx["pippo"] = 0
	m.Unlock()

	go increaser()
	//time.Sleep(2*time.Second)
	go increaser()

	time.Sleep(100*time.Second)

}

*/

var mutexTopic map[string]sync.Mutex

var c int

func increaser() {

	i := mutexTopic["pippo"]

	for {

		i.Lock()
		c = c + 1
		time.Sleep(1 * time.Second)
		fmt.Printf("Increased c: %d\n", c)
		i.Unlock()

	}

}

func main() {

	//adding topoc
	mutexTopic = make(map[string]sync.Mutex)
	c = 0

	mutexTopic["pippo"] = sync.Mutex{}

	go increaser()
	//time.Sleep(2*time.Second)
	go increaser()

	time.Sleep(100 * time.Second)

}
