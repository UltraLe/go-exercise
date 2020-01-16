package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

type QueueManager int

const (
	QUEUE_LEN               = 100
	CONSUMER_SERVICE_METHOD = "Consumer.Consume"
	INVISIBLE               = 0
	VISIBLE                 = 1
	SENT                    = 2
)

type QueueElement struct {
	message string
	visible map[int]int //a map of consumer id to its value of visibility
	//for that message
	lastTimeVisible int
}

func (q *QueueManager) Subscribe(ipPort string, reply *string) error {

	addr := strings.Split(ipPort, ":")

	mutexConsumer.Lock()
	consumerID := consumerNum
	consumerNum++
	consumerList = append(consumerList, ConsumerInfo{addr[0], addr[1], consumerID})
	mutexConsumer.Unlock()

	fmt.Printf("Subscribe requested by consumer: %s:%s\n", addr[0], addr[1])

	*reply = "Accepted"

	return nil
}

func (q *QueueManager) Unsubscribe(ipPort string, reply *string) error {

	addr := strings.Split(ipPort, ":")

	mutexConsumer.Lock()
	consumerNum--
	for i, consumer := range consumerList {
		if consumer.port == addr[1] && consumer.ip == addr[0] {
			*reply = "Removed"
			//this is not a critical section as long as
			//the RPC server is sequential. It could happen that
			//a consumer Unsubscribe but its unsubscribe request
			//comes after a publish request of a producer.
			consumerList[i] = consumerList[len(consumerList)-1]
			consumerList = consumerList[:len(consumerList)-1]
		}
	}
	mutexConsumer.Unlock()
	return nil
}

func (q *QueueManager) Publish(message string, reply *string) error {

	//actual implementation: if there are no consumer, reject the message
	if consumerNum == 0 {
		return nil
	}

	indx := findPosInQueue()

	//if queue is full
	if indx == -1 {
		fmt.Println("\tWARNING: QUEUE IS FULL !")
		*reply = "Queue is full !"
		return nil
	}

	Queue[indx].lastTimeVisible = time.Now().Second()
	Queue[indx].message = message
	Queue[indx].visible = make(map[int]int)

	//in this way each message will be sent to the consumer that
	//are connected 'in this moment' to the broker
	mutexConsumer.Lock()
	for _, c := range consumerList {
		Queue[indx].visible[c.ID] = VISIBLE
	}

	for i := 0; i < consumerNum; i++ {
		go sendToConsumer(indx, consumerList[i])
	}
	mutexConsumer.Unlock()

	return nil
}

var TIME_OUT int
var port string
var Queue [QUEUE_LEN]QueueElement

var consumerList []ConsumerInfo
var mutexConsumer sync.Mutex
var consumerNum int

type ConsumerInfo struct {
	ip   string
	port string
	ID   int
}

func findPosInQueue() int {

	for i, qe := range Queue {
		if qe.lastTimeVisible == -1 {
			return i
		}
	}

	//if i'm here the queue is full
	return -1
}

func sendToConsumer(queueElementIndex int, consumer ConsumerInfo) {

	//changing visibility of the message for the current consumer to invisible
	Queue[queueElementIndex].visible[consumer.ID] = INVISIBLE

	client, err := rpc.Dial("tcp", consumer.ip+":"+consumer.port)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	var reply string

	//N.B.: the TIME OUT VALUE has to be chosen in order to avoid
	//situations where i could send a message more than once.
	//for simplicity, i am not handling this case.

	//this case could be handled by the consumer

	err = client.Call(CONSUMER_SERVICE_METHOD, Queue[queueElementIndex].message, &reply)

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	//if i am not here the message has not arrived to the consumer
	//and its value for its consumer remains 'invisible'

	if reply == "ACK" {
		//if i'm here the massage has been correctly delivered
		Queue[queueElementIndex].visible[consumer.ID] = SENT

		//now each consumer will check if the message could be eliminated
		//by checking that all the values on the 'visible' map are 'sent'

		del := true
		for _, v := range Queue[queueElementIndex].visible {
			//v is the value (invisible, sent, visible) of the message, for each consumer

			if v != SENT {
				del = false
				break
			}
		}

		if del {
			//i am not using the synchronization here, this means
			//that when scanning, more than one go routine can
			//realize that all the others has sent the message
			//This means that more than one routine could set
			//the lastTimeVisible = -1.
			//It would have been a problem if each of
			//them wanted to set a different value
			Queue[queueElementIndex].lastTimeVisible = -1
			fmt.Println("A message has been sent to all consumers, therefore deleted from the queue")
		}
	}
}

func getConsumer(ID int) ConsumerInfo {

	var c_nil ConsumerInfo
	//the consumer could have been unsubscribed
	//and could not be present anymore
	c_nil.ID = -1

	mutexConsumer.Lock()
	for _, c := range consumerList {
		if c.ID == ID {
			mutexConsumer.Unlock()
			return c
		}
	}
	mutexConsumer.Unlock()

	//if am here i have to notify the caller that the consumer
	//han not been found
	return c_nil
}

//go routine that will implement the 'timed out based' delivery semantic
func TimeOutChecker() {

	var now int

	for {
		time.Sleep(time.Second * time.Duration(TIME_OUT))

		now = time.Now().Second()

		for queueIndx, qe := range Queue {

			//if there is no message for that element
			if qe.lastTimeVisible == -1 {
				continue
			}

			messTO := qe.lastTimeVisible + TIME_OUT

			//if TO oof the queue element has expired, then send again the message
			if now >= messTO {

				qe.lastTimeVisible = time.Now().Second()

				//send the queue element to the consumer that has not received it
				for consID, vis := range qe.visible {

					c := getConsumer(consID)
					if c.ID == -1 {
						//if the consumer has unsubscribed in the meanwhile
						//just skip it
						continue
					}
					if vis != SENT {
						go sendToConsumer(queueIndx, c)
						//here we could handle eventual prediction of
						//crashed consumer by increasing a counter that
						//indicates number of retransmissions. When the number
						//is grater then a fixed value the consumer could be considered crashed.
					}
				}

			}
		}
	}
}

func serverRPC() {

	//the consumer has to send a message in which they specify
	//the port and  the ip address that are going to be used
	//by the broker to send them the message of the queue

	//setting up RPC server
	inbound, err := net.Listen("tcp", ":"+port)

	if err != nil {
		log.Fatal(err)
	}

	go TimeOutChecker()

	queueManager := new(QueueManager)
	rpc.Register(queueManager)

	rpc.Accept(inbound)
}

func QueueStatus() {

	for i, q := range Queue {

		if q.lastTimeVisible == -1 {
			continue
		}

		fmt.Println("--------------------------------------------------------------------")
		fmt.Printf("Queue Element n.%d\n", i)
		fmt.Printf("Message: %s\n", q.message)
		fmt.Println("Message status for the consumer:")
		for cid, v := range q.visible {
			c := getConsumer(cid)
			fmt.Printf("Consumer (ID: %d) %s:%s --> Message ", cid, c.ip, c.port)
			switch v {
			case INVISIBLE:
				fmt.Println("has been sent")
				break
			case VISIBLE:
				fmt.Println("has not been sent yet")
				break
			case SENT:
				fmt.Println("has been received")
				break
			}
		}
		fmt.Println("--------------------------------------------------------------------")
	}

}

func main() {

	if len(os.Args) < 2 || (os.Args[1] != "1" && os.Args[1] != "0") {
		fmt.Println("./broker <mode>\n(mode=0 for automatic test, mode=1 for manual test)")
		return
	}

	if os.Args[1] == "1" {
		fmt.Println("Insert time out value (in seconds): ")
		fmt.Scanf("%d", &TIME_OUT)

		fmt.Println("Insert RPC server PORT: ")
		fmt.Scanf("%s", &port)

	} else {
		//if automatic mode is chosen, a fixed port and TIME_OUT value
		//will be set. N.B.: the time out value could be updated later.
		TIME_OUT = 5
		port = "12345"
	}

	consumerNum = 0

	//setting up queue, 'lastTimeVisible' set to -1
	for i := 0; i < QUEUE_LEN; i++ {

		//qe.visible = []bool{}
		//the set up of the array of bool depends on how many
		//consumer there are, so it has to be done by the InsertMessage routine
		Queue[i].lastTimeVisible = -1
	}

	go serverRPC()

	var choice int

	for {
		fmt.Println("What do you want to do ?")
		fmt.Println("1)Show the status of the messages")
		fmt.Println("2)Update TO value")
		fmt.Println("3)Exit")

		fmt.Scanf("%d", &choice)

		switch choice {
		case 1:
			QueueStatus()
			break
		case 2:
			fmt.Scanf("%d", &TIME_OUT)
			break
		case 3:
			return
		default:
			fmt.Println("What ?")
		}
	}
}
