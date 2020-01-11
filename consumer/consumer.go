package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

type Consumer int

func (c *Consumer) Consume(mess string, reply *string) error {

	consumedMessages = append(consumedMessages, mess)
	fmt.Println("MEssage arrived: " + mess)
	*reply = "ACK"

	return nil
}

var consumedMessages []string

func ConsumeMessages() {
	fmt.Println("Message list:")
	for i, m := range consumedMessages {
		fmt.Printf("%d] %s\n", i, m)
	}
	fmt.Println("End of message list")

	//consumedMessages = make([]string, MAX_MESSAGES)
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

func SubscribeToBroker() {

	var ip, brokPort string
	fmt.Println("Insert broker IP: ")
	fmt.Scanf("%s:%s", &ip)
	fmt.Println("Insert broker Port: ")
	fmt.Scanf("%s", &brokPort)

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

	//TODO listen only from subscribed broker
	rpc.Accept(inbound)
}

var myIp, port string

//go routine that will permit the user to interact with the consumers
func Selection() {

	var choice int

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
			SubscribeToBroker()
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
	Selection()
}
