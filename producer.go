package main

import (
	"coda/producerStuff"
	"fmt"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

func dialRR1(mess, ip, port string) {

	var reply string

	client, err := rpc.Dial("tcp", ip+":"+port)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if producerStuff.RR1Mode == producerStuff.TCP_STYLE {

		for sec := 1; sec <= producerStuff.MAX_SEC; sec = sec * 2 {

			err = client.Call(producerStuff.ServiceMethod, mess, &reply)

			if err != nil && sec == producerStuff.MAX_SEC {
				fmt.Println("Could not send the message\n" + err.Error())
				return
			} else if err == nil && sec < producerStuff.MAX_SEC {
				break
			} else {
				time.Sleep(time.Duration(sec))
				continue
			}
		}

	} else if producerStuff.RR1Mode == producerStuff.BRUTE_FORCE_STYLE {

		for {
			err = client.Call(producerStuff.ServiceMethod, mess, &reply)

			if err == nil {
				break
			}
		}

	} else if producerStuff.RR1Mode == producerStuff.MAX_TRIES_STYLE {

		for i := 1; i <= producerStuff.MaxTimes; i++ {

			err = client.Call(producerStuff.ServiceMethod, mess, &reply)
			if err != nil && i == producerStuff.MaxTimes {
				fmt.Println("Could not send the message\n" + err.Error())
				return
			} else if err == nil {
				break
			} else {
				continue
			}
		}

	} else {

		fmt.Println("Invalid RR1Mode inserted")
		return
	}
}

//the actual function that will listen to a given channel
//and when a message is received the dialRR1 mechanism will start
//there will be as many go routine of the publish func as the brokers connected to the producerStuff
func publish(info producerStuff.BrokerInfo) {

	for {
		select {
		case cmd := <-info.CtrlChannel:
			if cmd == producerStuff.GOR_EXIT {
				return
			}
		case msg := <-info.MessagesChannel:
			dialRR1(msg, info.Ip, info.Port)
		}
	}

}

func addBroker(ip, port string) {

	ch := make(chan string)
	ctrlCh := make(chan int)

	producerStuff.BrokerList = append(producerStuff.BrokerList, producerStuff.BrokerInfo{ip, port, ch, ctrlCh})

	//create go routine associated to the brokerStuff
	go publish(producerStuff.BrokerList[len(producerStuff.BrokerList)-1])

}

func removeBroker() {

	var choice int

	fmt.Println("Which brokerStuff do you want to delete ?")
	printBrokList()

	fmt.Println("Your choice: ")
	fmt.Scanf("%d", &choice)
	fmt.Printf("You've choosed %d\n", choice)

	//send kill message to kill the goroutine associated to the brokerStuff.... work on this
	producerStuff.BrokerList[choice].CtrlChannel <- producerStuff.GOR_EXIT

	close(producerStuff.BrokerList[choice].CtrlChannel)
	close(producerStuff.BrokerList[choice].MessagesChannel)

	producerStuff.BrokerList[choice] = producerStuff.BrokerList[len(producerStuff.BrokerList)-1] // Copy last element to index i.
	producerStuff.BrokerList = producerStuff.BrokerList[:len(producerStuff.BrokerList)-1]        // Truncate slice

}

func printBrokList() {

	fmt.Println("Broker list:")
	for indx, pl := range producerStuff.BrokerList {
		fmt.Printf("%d) %s:%s\n", indx, pl.Ip, pl.Port)
	}

}

func rr1ModeSelector() {

	for {
		fmt.Println("What type of request retransmit do you want to use ?")

		fmt.Println("1)TCP_STYLE")
		fmt.Println("2)BRUTE_FORCE_STYLE")
		fmt.Println("3)MAX_TRIES_STYLE")

		fmt.Scanf("%d", &producerStuff.RR1Mode)

		if producerStuff.RR1Mode < 1 && producerStuff.RR1Mode > 3 {
			fmt.Println("Invalid input, retry")
			continue
		}

		if producerStuff.RR1Mode == producerStuff.MAX_TRIES_STYLE {
			fmt.Println("How many times do you want to try to reach the queque ?")
			fmt.Scanf("%d", &producerStuff.MaxTimes)
		}

		break
	}

}

//go routine that will take messages from stdin
//and send to all the channels of all the brokerStuff that the producerStuff is connected to
func manualModeProd() {

	var choice int
	var message, ip, port string

	producerStuff.ServiceMethod = producerStuff.PUBLISH_SERVICE_METHOD

	rr1ModeSelector()

	for {
		fmt.Println("What do you want to do ?")
		fmt.Println("1)Print printBrokList")
		fmt.Println("2)Remove a Broker")
		fmt.Println("3)Add a Broker")
		fmt.Println("4)publish a message")
		fmt.Println("5)Select a RR1 Mode")
		fmt.Println("6)Exit")

		fmt.Scanf("%d", &choice)

		switch choice {
		case 1:
			printBrokList()
			break
		case 2:
			removeBroker()
			break
		case 3:
			fmt.Println("Insert brokerStuff IP: ")
			fmt.Scanf("%s:%s", &ip)
			fmt.Println("Insert brokerStuff Port: ")
			fmt.Scanf("%s", &port)
			addBroker(ip, port)
			break
		case 4:
			fmt.Println("Insert the message that you want to produce: ")
			fmt.Scanf("%s", &message)

			//Inserting message in all brokerStuff's channels
			for _, p := range producerStuff.BrokerList {
				p.MessagesChannel <- message
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
		fmt.Println("./producerStuff <mode>\n(mode=0 for automatic test, mode=1 for manual test)")
		return
	}

	if os.Args[1] == "1" {
		manualModeProd()
	}

	SECS := 5
	messageSent := 1

	fmt.Printf("\n\tProducer started\n\n")

	//The automatic test of the brokerStuff and the consumerStuff must begin first.

	//During the test the producerStuff will generate each SECS seconds
	//the fixed message 'Debug-#messageSent'.
	//The producerStuff will publish message to a single brokerStuff on port 12345 (in TCP_STYLE).
	producerStuff.RR1Mode = producerStuff.TCP_STYLE
	producerStuff.ServiceMethod = producerStuff.PUBLISH_SERVICE_METHOD
	addBroker("0.0.0.0", "12345")
	for {
		time.Sleep(time.Second * time.Duration(SECS))

		message := "TestMessage n." + strconv.Itoa(messageSent)
		fmt.Println("Producing message: ", message)

		for _, p := range producerStuff.BrokerList {
			p.MessagesChannel <- message
		}

		messageSent += 1

	}

}
