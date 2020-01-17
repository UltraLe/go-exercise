package main

import (
	"fmt"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

type BrokerInfo struct {

	//ip and port are going to be read and used by the producer
	//to establish a connection with the broker
	ip              string
	port            string
	messagesChannel chan string
	ctrlChannel     chan int
}

const (
	TCP_STYLE         = 1 //each request is sent every 1s, 2s, 4s, 8s... MAX_SEC
	BRUTE_FORCE_STYLE = 2 //never stop trying to send the message
	MAX_TRIES_STYLE   = 3 //do at most MAX_TRIES_STYLE tries

	PUBLISH_SERVICE_METHOD = "QueueManager.Publish"
	MAX_SEC                = 64
	GOR_EXIT               = 0 //used to request the closure of the "Publish" go routine
)

var brokerList []BrokerInfo

var serviceMethod string
var rr1Mode int
var maxTimes int

func DialRR1(mess, ip, port string) {

	var reply string

	client, err := rpc.Dial("tcp", ip+":"+port)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if rr1Mode == TCP_STYLE {

		for sec := 1; sec <= MAX_SEC; sec = sec * 2 {

			err = client.Call(serviceMethod, mess, &reply)

			if err != nil && sec == MAX_SEC {
				fmt.Println("Could not send the message\n" + err.Error())
				return
			} else if err == nil && sec < MAX_SEC {
				break
			} else {
				time.Sleep(time.Duration(sec))
				continue
			}
		}

	} else if rr1Mode == BRUTE_FORCE_STYLE {

		for {
			err = client.Call(serviceMethod, mess, &reply)

			if err == nil {
				break
			}
		}

	} else if rr1Mode == MAX_TRIES_STYLE {

		for i := 1; i <= maxTimes; i++ {

			err = client.Call(serviceMethod, mess, &reply)
			if err != nil && i == maxTimes {
				fmt.Println("Could not send the message\n" + err.Error())
				return
			} else if err == nil {
				break
			} else {
				continue
			}
		}

	} else {

		fmt.Println("Invalid rr1Mode inserted")
		return
	}
}

//the actual function that will listen to a given channel
//and when a message is received the DialRR1 mechanism will start
//there will be as many go routine of the Publish func as the brokers connected to the producer
func Publish(info BrokerInfo) {

	for {
		select {
		case cmd := <-info.ctrlChannel:
			if cmd == GOR_EXIT {
				return
			}
		case msg := <-info.messagesChannel:
			DialRR1(msg, info.ip, info.port)
		}
	}

}

func AddBroker(ip, port string) {

	ch := make(chan string)
	ctrlCh := make(chan int)

	brokerList = append(brokerList, BrokerInfo{ip, port, ch, ctrlCh})

	//create go routine associated to the broker
	go Publish(brokerList[len(brokerList)-1])

}

func RemoveBroker() {

	var choice int

	fmt.Println("Which broker do you want to delete ?")
	BrokerList()

	fmt.Println("Your choice: ")
	fmt.Scanf("%d", &choice)
	fmt.Printf("You've choosed %d\n", choice)

	//send kill message to kill the goroutine associated to the broker.... work on this
	brokerList[choice].ctrlChannel <- GOR_EXIT

	close(brokerList[choice].ctrlChannel)
	close(brokerList[choice].messagesChannel)

	brokerList[choice] = brokerList[len(brokerList)-1] // Copy last element to index i.
	brokerList = brokerList[:len(brokerList)-1]        // Truncate slice

}

func BrokerList() {

	fmt.Println("Broker list:")
	for indx, pl := range brokerList {
		fmt.Printf("%d) %s:%s\n", indx, pl.ip, pl.port)
	}

}

func rr1ModeSelector() {

	for {
		fmt.Println("What type of request retransmit do you want to use ?")

		fmt.Println("1)TCP_STYLE")
		fmt.Println("2)BRUTE_FORCE_STYLE")
		fmt.Println("3)MAX_TRIES_STYLE")

		fmt.Scanf("%d", &rr1Mode)

		if rr1Mode < 1 && rr1Mode > 3 {
			fmt.Println("Invalid input, retry")
			continue
		}

		if rr1Mode == MAX_TRIES_STYLE {
			fmt.Println("How many times do you want to try to reach the queque ?")
			fmt.Scanf("%d", &maxTimes)
		}

		break
	}

}

//go routine that will take messages from stdin
//and send to all the channels of all the broker that the producer is connected to
func ManualMode() {

	var choice int
	var message, ip, port string

	serviceMethod = PUBLISH_SERVICE_METHOD

	rr1ModeSelector()

	for {
		fmt.Println("What do you want to do ?")
		fmt.Println("1)Print BrokerList")
		fmt.Println("2)Remove a Broker")
		fmt.Println("3)Add a Broker")
		fmt.Println("4)Publish a message")
		fmt.Println("5)Select a RR1 Mode")
		fmt.Println("6)Exit")

		fmt.Scanf("%d", &choice)

		switch choice {
		case 1:
			BrokerList()
			break
		case 2:
			RemoveBroker()
			break
		case 3:
			fmt.Println("Insert broker IP: ")
			fmt.Scanf("%s:%s", &ip)
			fmt.Println("Insert broker Port: ")
			fmt.Scanf("%s", &port)
			AddBroker(ip, port)
			break
		case 4:
			fmt.Println("Insert the message that you want to produce: ")
			fmt.Scanf("%s", &message)

			//Inserting message in all broker's channels
			for _, p := range brokerList {
				p.messagesChannel <- message
			}
			break
		case 5:
			rr1ModeSelector()
		case 6:
			return

		default:
			fmt.Println("What ?")
		}
	}
}

func main() {

	if len(os.Args) < 2 || (os.Args[1] != "1" && os.Args[1] != "0") {
		fmt.Println("./producer <mode>\n(mode=0 for automatic test, mode=1 for manual test)")
		return
	}

	if os.Args[1] == "1" {
		ManualMode()
	}

	SECS := 5
	messageSent := 1

	fmt.Printf("\n\tProducer started in automatic mode\n")

	//The automatic test of the broker and the consumer must begin first.

	//During the test the producer will generate each SECS seconds
	//the fixed message 'Debug-#messageSent'.
	//The producer will publish message to a single broker on port 12345 (in TCP_STYLE).
	rr1Mode = TCP_STYLE
	serviceMethod = PUBLISH_SERVICE_METHOD
	AddBroker("0.0.0.0", "12345")
	for {
		time.Sleep(time.Second * time.Duration(SECS))

		message := "TestMessage n." + strconv.Itoa(messageSent)
		fmt.Println("Producing message: ", message)

		for _, p := range brokerList {
			p.messagesChannel <- message
		}

		messageSent += 1

	}

}
