package brokerStuff

import (
	"net/rpc"
	"time"
)

//at least once semantic delivery
func sendToConsumerALO(queueElementIndex int, consumer ConsumerInfo) {

	client, err := rpc.Dial("tcp", consumer.Ip+":"+consumer.Port)
	if err != nil {
		//fmt.Println(err.Error())
		return
	}

	var reply string
	done := false

	for {
		aloCall := client.Go(CONSUMER_SERVICE_METHOD, Queue[queueElementIndex].Message, &reply, nil)
		timer := time.NewTimer(time.Second * ALO_TIMEOUT)

		select {
		case <-aloCall.Done:
			//if the call has been done correctly
			if reply == "ACK" {
				done = true
			}
			break
		case <-timer.C:
			//it the after ALO_TIMEOUT seconds the call has not done yet
			//then make it again
			break
		}

		if done {
			break
		}
	}
}

//time out based delivery semantic
func SendToConsumerTOB(queueElementIndex int, consumer ConsumerInfo) {

	//changing visibility of the message for the current consumerStuff to invisible
	Queue[queueElementIndex].Visible[consumer.ID] = INVISIBLE

	client, err := rpc.Dial("tcp", consumer.Ip+":"+consumer.Port)

	if err != nil {
		//fmt.Println(err.Error())
		return
	}

	var reply string

	//N.B.: the TIME OUT VALUE has to be chosen in order to avoid
	//situations where i could send a message more than once.
	//for simplicity, i am not handling this case.

	err = client.Call(CONSUMER_SERVICE_METHOD, Queue[queueElementIndex].Message, &reply)

	if err != nil {
		//fmt.Println(err.Error())
		return
	}

	//if i am not here the message has not arrived to the consumerStuff
	//and its value for its consumerStuff remains 'invisible'

	if reply == "ACK" {
		//if i'm here the massage has been correctly delivered
		Queue[queueElementIndex].Visible[consumer.ID] = SENT

		//now each consumerStuff will check if the message could be eliminated
		//by checking that all the values on the 'visible' map are 'sent'

		del := true
		for _, v := range Queue[queueElementIndex].Visible {
			//v is the value (invisible, sent, visible) of the message, for each consumerStuff

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
			Queue[queueElementIndex].LastTimeVisible = -1
		}
	}
}
