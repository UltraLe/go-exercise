package brokerStuff

import (
	"fmt"
	"strings"
	"time"
)

type QueueManager int

func (q *QueueManager) Subscribe(ipPort string, reply *string) error {

	addr := strings.Split(ipPort, ":")

	MutexConsumer.Lock()

	//if the consumerStuff has already present in the consumerStuff list,
	//maybe because it has crashed then recovered and re-subscribed,
	//it must not be inserted again into the list.
	for _, c := range ConsumerList {
		if c.Ip == addr[0] && c.Port == addr[1] {
			MutexConsumer.Unlock()
			fmt.Printf("The consumerStuff %s:%s has been recovered\n", c.Ip, c.Port)
			*reply = "Accepted"
			return nil
		}
	}
	consumerID := ConsumerNum
	ConsumerNum++
	ConsumerList = append(ConsumerList, ConsumerInfo{addr[0], addr[1], consumerID})
	MutexConsumer.Unlock()

	fmt.Printf("Subscribe requested by consumerStuff: %s:%s\n", addr[0], addr[1])

	*reply = "Accepted"
	return nil
}

func (q *QueueManager) Unsubscribe(ipPort string, reply *string) error {

	addr := strings.Split(ipPort, ":")

	MutexConsumer.Lock()
	ConsumerNum--
	for i, consumer := range ConsumerList {
		if consumer.Port == addr[1] && consumer.Ip == addr[0] {
			*reply = "Removed"
			//this is not a critical section as long as
			//the RPC server is sequential. It could happen that
			//a consumerStuff Unsubscribe but its unsubscribe request
			//comes after a publish request of a producerStuff.
			ConsumerList[i] = ConsumerList[len(ConsumerList)-1]
			ConsumerList = ConsumerList[:len(ConsumerList)-1]
		}
	}
	MutexConsumer.Unlock()
	return nil
}

func (q *QueueManager) Publish(message string, reply *string) error {

	//actual implementation: if there are no consumerStuff, reject the message
	if ConsumerNum == 0 {
		return nil
	}

	indx := FindPosInQueue()

	//if queue is full
	if indx == -1 {
		fmt.Println("\tWARNING: QUEUE IS FULL !")
		*reply = "Queue is full !"
		return nil
	}

	Queue[indx].Message = message

	if ChosenSemantic == TOB {
		Queue[indx].LastTimeVisible = int(time.Now().Unix())
		Queue[indx].Visible = make(map[int]int)

		MutexConsumer.Lock()
		for _, c := range ConsumerList {
			//in this way each message will be sent to the consumerStuff that
			//are connected 'in this moment' to the brokerStuff
			Queue[indx].Visible[c.ID] = VISIBLE
		}

		for i := 0; i < ConsumerNum; i++ {
			go SendToConsumerTOB(indx, ConsumerList[i])
		}
		MutexConsumer.Unlock()

	} else if ChosenSemantic == ALO {

		MutexConsumer.Lock()
		for i := 0; i < ConsumerNum; i++ {
			go sendToConsumerALO(indx, ConsumerList[i])
		}
		MutexConsumer.Unlock()
	}

	return nil
}

func FindPosInQueue() int {

	for i, qe := range Queue {
		if qe.LastTimeVisible == -1 {
			return i
		}
	}

	//if i'm here the queue is full
	return -1
}
