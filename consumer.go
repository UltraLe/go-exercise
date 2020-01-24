package main

import (
	"coda/consumerStuff"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

func ConsumeMessages() {

	consumerStuff.Mutex.Lock()
	fmt.Println("Message list:")
	for i, m := range consumerStuff.ConsumedMessages {
		fmt.Printf("%d] %s\n", i, m)
	}
	fmt.Println("End of message list")
	consumerStuff.Mutex.Unlock()

}

func subscribeToBroker(ip, brokPort string) {

	consumerStuff.BrokerList = append(consumerStuff.BrokerList, consumerStuff.BrokerInfo{ip, brokPort})

	client, err := rpc.Dial("tcp", ip+":"+brokPort)

	if err != nil {
		fmt.Println("Could not connect to the brokerStuff\n+" + err.Error())
		return
	}

	var reply string

	err = client.Call(consumerStuff.SUBSCRIBE_SERVICE_METHOD, consumerStuff.MyIp+":"+consumerStuff.Port, &reply)

	if err != nil {
		fmt.Println("Could not subscribe to the brokerStuff\n+" + err.Error())
		return
	}

	if reply == "Accepted" {
		fmt.Println("Correctly subscribed")
	}

}

func unsubscribeFromBroker() {

	var choice int

	fmt.Println("Which brokerStuff do you want to unsubscribe to ?")
	printBrokerList()

	fmt.Println("Your choice: ")
	fmt.Scanf("%d", &choice)
	fmt.Printf("You've choosed %d\n", choice)

	client, err := rpc.Dial("tcp", consumerStuff.BrokerList[choice].Ip+":"+consumerStuff.BrokerList[choice].Port)

	if err != nil {
		fmt.Println("Could not connect to the brokerStuff\n+" + err.Error())
		return
	}

	var reply string
	err = client.Call(consumerStuff.UNSUBSCRIBE_SERVICE_METHOD, consumerStuff.MyIp+":"+consumerStuff.Port, &reply)

	if err != nil {
		fmt.Println("Could not unsubscribe to the brokerStuff\n+" + err.Error())
		return
	}

	if reply == "Removed" {
		fmt.Println("Correctly unsubscribed")
	}

	consumerStuff.BrokerList[choice] = consumerStuff.BrokerList[len(consumerStuff.BrokerList)-1] // Copy last element to index i.
	consumerStuff.BrokerList = consumerStuff.BrokerList[:len(consumerStuff.BrokerList)-1]        // Truncate slice

}

func printBrokerList() {

	fmt.Println("Broker list:")
	for indx, pl := range consumerStuff.BrokerList {
		fmt.Printf("%d) %s:%s\n", indx, pl.Ip, pl.Port)
	}

}

func serverRPCcons() {

	//setting up RPC server
	inbound, err := net.Listen("tcp", consumerStuff.MyIp+":"+consumerStuff.Port)

	if err != nil {
		log.Fatal(err)
	}

	consumer := new(consumerStuff.Consumer)
	rpc.Register(consumer)

	rpc.Accept(inbound)
}

//go routine that will permit the user to interact with the consumers
func manualMode() {

	var choice int
	var brokPort, brokIP string

	fmt.Println("Insert RPC server IP for the consumerStuff: ")
	fmt.Scanf("%s", &consumerStuff.MyIp)

	fmt.Println("Insert RPC server PORT for the consumerStuff: ")
	fmt.Scanf("%s", &consumerStuff.Port)

	go serverRPCcons()

	for {
		fmt.Println("What do you want to do ?")
		fmt.Println("1)Print Brokers that your consumerStuff are subscribed to")
		fmt.Println("2)Unsubscribe from a Broker")
		fmt.Println("3)Subscribe to a Broker")
		fmt.Println("4)Print message received from the brokers")
		fmt.Println("5)Exit")

		fmt.Scanf("%d", &choice)

		switch choice {
		case 1:
			printBrokerList()
			break
		case 2:
			unsubscribeFromBroker()
			break
		case 3:
			fmt.Println("Insert brokerStuff IP: ")
			fmt.Scanf("%s:%s", &brokIP)
			fmt.Println("Insert brokerStuff Port: ")
			fmt.Scanf("%s", &brokPort)
			subscribeToBroker(brokIP, brokPort)
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
		fmt.Println("./consumerStuff <mode>")
		fmt.Println("(mode=0 for automatic test, mode=1 for manual test, mode=2 + <port> automatic test with given port)")
		return
	}

	if os.Args[1] == "1" {
		manualMode()
	}

	UPDATE_SEC := 5

	//The automatic test of the brokerStuff must begin first.
	//During the automatic test the consumerStuff will show the messages that has received
	//every UPDATE_SEC seconds

	consumerStuff.MyIp = "0.0.0.0"
	rand.Seed(12345)

	if os.Args[1] == "2" {
		consumerStuff.Port = os.Args[2]
	} else {
		consumerStuff.Port = strconv.Itoa(rand.Intn(65536-1025)*(time.Now().Second())%65536 + 1025)
	}

	fmt.Printf("\n\tConsumer %s:%s started\n\n", consumerStuff.MyIp, consumerStuff.Port)

	go serverRPCcons()
	subscribeToBroker("0.0.0.0", "12345")
	for {
		time.Sleep(time.Duration(UPDATE_SEC) * time.Second)
		ConsumeMessages()
	}
}
