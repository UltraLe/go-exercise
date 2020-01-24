package brokerStuff

import "sync"

const (
	QUEUE_LEN               = 100
	CONSUMER_SERVICE_METHOD = "Consumer.Consume"
	INVISIBLE               = 0
	VISIBLE                 = 1
	SENT                    = 2
	TOB                     = 0
	ALO                     = 1
	ALO_TIMEOUT             = 3
)

type ConsumerInfo struct {
	Ip   string
	Port string
	ID   int
}

type QueueElement struct {
	Message string
	Visible map[int]int //a map of consumerStuff id to its value of visibility
	//for that message
	LastTimeVisible int
}

var TIME_OUT int
var Port string
var ChosenSemantic int
var Queue [QUEUE_LEN]QueueElement

var ConsumerList []ConsumerInfo
var MutexConsumer sync.Mutex
var ConsumerNum int
