package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Consumer int

func (c *Consumer) Consume(mess string, reply *string) error {

	mutex.Lock()
	consumedMessages = append(consumedMessages, mess)
	mutex.Unlock()

	*reply = "ACK"

	return nil
}

var consumedMessages []string
var mutex sync.Mutex

func ConsumeMessages() {

	mutex.Lock()
	fmt.Println("Message list:")
	for i, m := range consumedMessages {
		fmt.Printf("%d] %s\n", i, m)
	}
	fmt.Println("End of message list")
	mutex.Unlock()

}

type BrokerInfo struct {
	//ip and port are going to be read and used by the consumer
	//to consume broker messages
	ip   string
	port string
}

const (
	SUBSCRIBE_SERVICE_METHOD   = "QueueManager.Subscribe"
	UNSUBSCRIBE_SERVICE_METHOD = "QueueManager.Unsubscribe"
)

var brokerList []BrokerInfo

func SubscribeToBroker(ip, brokPort string) {

	brokerList = append(brokerList, BrokerInfo{ip, brokPort})

	client, err := rpc.Dial("tcp", ip+":"+brokPort)

	if err != nil {
		fmt.Println("Could not connect to the broker\n+" + err.Error())
		return
	}

	var reply string

	err = client.Call(SUBSCRIBE_SERVICE_METHOD, myIp+":"+port, &reply)

	if err != nil {
		fmt.Println("Could not subscribe to the broker\n+" + err.Error())
		return
	}

	if reply == "Accepted" {
		fmt.Println("Correctly subscribed")
	}

}

func UnsubscribeFromBroker() {

	var choice int

	fmt.Println("Which broker do you want to unsubscribe to ?")
	BrokerList()

	fmt.Println("Your choice: ")
	fmt.Scanf("%d", &choice)
	fmt.Printf("You've choosed %d\n", choice)

	client, err := rpc.Dial("tcp", brokerList[choice].ip+":"+brokerList[choice].port)

	if err != nil {
		fmt.Println("Could not connect to the broker\n+" + err.Error())
		return
	}

	var reply string
	err = client.Call(UNSUBSCRIBE_SERVICE_METHOD, myIp+":"+port, &reply)

	if err != nil {
		fmt.Println("Could not unsubscribe to the broker\n+" + err.Error())
		return
	}

	if reply == "Removed" {
		fmt.Println("Correctly unsubscribed")
	}

	brokerList[choice] = brokerList[len(brokerList)-1] // Copy last element to index i.
	brokerList = brokerList[:len(brokerList)-1]        // Truncate slice

}

func BrokerList() {

	fmt.Println("Broker list:")
	for indx, pl := range brokerList {
		fmt.Printf("%d) %s:%s\n", indx, pl.ip, pl.port)
	}

}

func serverRPC() {

	//setting up RPC server
	inbound, err := net.Listen("tcp", myIp+":"+port)

	if err != nil {
		log.Fatal(err)
	}

	consumer := new(Consumer)
	rpc.Register(consumer)

	rpc.Accept(inbound)
}

var myIp, port string

//go routine that will permit the user to interact with the consumers
func ManualMode() {

	var choice int
	var brokPort, brokIP string

	fmt.Println("Insert RPC server IP for the consumer: ")
	fmt.Scanf("%s", &myIp)

	fmt.Println("Insert RPC server PORT for the consumer: ")
	fmt.Scanf("%s", &port)

	go serverRPC()

	for {
		fmt.Println("What do you want to do ?")
		fmt.Println("1)Print Brokers that your consumer are subscribed to")
		fmt.Println("2)Unsubscribe from a Broker")
		fmt.Println("3)Subscribe to a Broker")
		fmt.Println("4)Print message received from the brokers")
		fmt.Println("5)Exit")

		fmt.Scanf("%d", &choice)

		switch choice {
		case 1:
			BrokerList()
			break
		case 2:
			UnsubscribeFromBroker()
			break
		case 3:
			fmt.Println("Insert broker IP: ")
			fmt.Scanf("%s:%s", &brokIP)
			fmt.Println("Insert broker Port: ")
			fmt.Scanf("%s", &brokPort)
			SubscribeToBroker(brokIP, brokPort)
			break
		case 4:
			ConsumeMessages()
			break
		case 5:
			return

		default:
			fmt.Println("What ?")
		}
	}
}

func main() {

	if os.Args[1] != "2" && os.Args[1] != "1" && os.Args[1] != "0" {
		fmt.Println("./consumer <mode>")
		fmt.Println("(mode=0 for automatic test, mode=1 for manual test, mode=2 + <port> automatic test with given port)")
		return
	}

	if os.Args[1] == "1" {
		ManualMode()
	}

	UPDATE_SEC := 5

	//The automatic test of the broker must begin first.
	//During the automatic test the consumer will show the messages that has received
	//every UPDATE_SEC seconds

	myIp = "0.0.0.0"
	rand.Seed(12345)

	if os.Args[1] == "2" {
		port = os.Args[2]
	} else {
		port = strconv.Itoa(rand.Intn(65536-1025)*(time.Now().Second())%65536 + 1025)
	}

	fmt.Printf("\n\tConsumer %s:%s started\n\n", myIp, port)

	go serverRPC()
	SubscribeToBroker("0.0.0.0", "12345")
	for {
		time.Sleep(time.Duration(UPDATE_SEC) * time.Second)
		ConsumeMessages()
	}
}
