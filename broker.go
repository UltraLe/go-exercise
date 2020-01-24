package main

import (
	"coda/brokerStuff"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"time"
)

func getConsumer(ID int) brokerStuff.ConsumerInfo {

	var c_nil brokerStuff.ConsumerInfo
	//the consumerStuff could have been unsubscribed
	//and could not be present anymore
	c_nil.ID = -1

	brokerStuff.MutexConsumer.Lock()
	for _, c := range brokerStuff.ConsumerList {
		if c.ID == ID {
			brokerStuff.MutexConsumer.Unlock()
			return c
		}
	}
	brokerStuff.MutexConsumer.Unlock()

	//if am here i have to notify the caller that the consumerStuff
	//han not been found
	return c_nil
}

//go routine that will implement the 'time out based' delivery semantic
func timeOutChecker() {

	var now int

	for {
		time.Sleep(time.Second * time.Duration(brokerStuff.TIME_OUT))

		now = int(time.Now().Unix())

		for queueIndx, qe := range brokerStuff.Queue {

			//if there is no message for that element
			if qe.LastTimeVisible == -1 {
				continue
			}

			messTO := qe.LastTimeVisible + brokerStuff.TIME_OUT

			//if TO oof the queue element has expired, then send again the message
			if now >= messTO {

				qe.LastTimeVisible = int(time.Now().Unix())

				//send the queue element to the consumerStuff that has not received it
				for consID, vis := range qe.Visible {

					c := getConsumer(consID)
					if c.ID == -1 {
						//if the consumerStuff has unsubscribed in the meanwhile
						//just skip it
						continue
					}
					if vis != brokerStuff.SENT {
						go brokerStuff.SendToConsumerTOB(queueIndx, c)
						//here we could handle eventual prediction of
						//crashed consumerStuff by increasing a counter that
						//indicates number of retransmissions. When the number
						//is grater then a fixed value the consumerStuff could be considered crashed.
					}
				}

			}
		}
	}
}

func serverRPC() {

	//the consumerStuff has to send a message in which they specify
	//the Port and  the ip address that are going to be used
	//by the brokerStuff to send them the message of the queue

	//setting up RPC server
	inbound, err := net.Listen("tcp", ":"+brokerStuff.Port)

	if err != nil {
		log.Fatal(err)
	}

	queueManager := new(brokerStuff.QueueManager)
	rpc.Register(queueManager)

	rpc.Accept(inbound)
}

func queueStatus() {

	fmt.Println("Queue Messages Status Start")

	for i, q := range brokerStuff.Queue {

		if q.LastTimeVisible == -1 {
			continue
		}

		fmt.Println("--------------------------------------------------------------------")
		fmt.Printf("Queue Element n.%d\n", i)
		fmt.Printf("Message: %s\n", q.Message)
		fmt.Println("Message status for the consumerStuff:")
		for cid, v := range q.Visible {
			c := getConsumer(cid)
			fmt.Printf("Consumer (ID: %d) %s:%s --> Message ", cid, c.Ip, c.Port)
			switch v {
			case brokerStuff.INVISIBLE:
				fmt.Println("has been sent")
				break
			case brokerStuff.VISIBLE:
				fmt.Println("has not been sent yet")
				break
			case brokerStuff.SENT:
				fmt.Println("has been received")
				break
			}
		}
		fmt.Println("--------------------------------------------------------------------")
	}

	fmt.Println("Queue Messages Status Stop")
	//if no element are showed, all the messages have been sent
	//the the consumerStuff subscribed to the queue

}

func main() {

	if len(os.Args) < 2 || (os.Args[1] != "1" && os.Args[1] != "0") {
		fmt.Println("./brokerStuff <mode>\n(mode=0 for automatic test, mode=1 for manual test)")
		return
	}

	if os.Args[1] == "1" {
		fmt.Println("Insert time out value (in seconds): ")
		fmt.Scanf("%d", &brokerStuff.TIME_OUT)

		fmt.Println("Insert RPC server PORT: ")
		fmt.Scanf("%s", &brokerStuff.Port)
		for {
			fmt.Println("Chose a delivery semantic:\n0)Time Out Based\n1)At Least Once\nYour choice: ")
			fmt.Scanf("%d", &brokerStuff.ChosenSemantic)
			if brokerStuff.ChosenSemantic != brokerStuff.TOB && brokerStuff.ChosenSemantic != brokerStuff.ALO {
				continue
			} else {
				break
			}
		}
	} else {
		//if automatic mode is chosen, the time out base delivery will be chosen
		//with a fixed Port and TIME_OUT value
		//will be set. N.B.: the time out value could be updated later.
		brokerStuff.TIME_OUT = 5
		brokerStuff.Port = "12345"
		brokerStuff.ChosenSemantic = brokerStuff.TOB
	}

	fmt.Printf("\n\tBroker listening on PORT %s started with Time Out Based Delivery semantic\n\n", brokerStuff.Port)

	brokerStuff.ConsumerNum = 0

	//setting up queue, 'lastTimeVisible' set to -1
	for i := 0; i < brokerStuff.QUEUE_LEN; i++ {

		//qe.visible = []bool{}
		//the set up of the array of bool depends on how many
		//consumerStuff there are, so it has to be done by the InsertMessage routine
		brokerStuff.Queue[i].LastTimeVisible = -1
	}

	go serverRPC()

	if brokerStuff.ChosenSemantic == brokerStuff.TOB {
		go timeOutChecker()
	}

	var choice int

	for {
		fmt.Println("What do you want to do ?")
		fmt.Println("1)Show the status of the messages")
		fmt.Println("2)Update TO value")
		fmt.Println("3)Exit")

		fmt.Scanf("%d", &choice)

		switch choice {
		case 1:
			queueStatus()
			break
		case 2:
			fmt.Scanf("%d", &brokerStuff.TIME_OUT)
			break
		case 3:
			return
		default:
			fmt.Println("What ?")
		}
	}
}
