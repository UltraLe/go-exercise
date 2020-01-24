package consumerStuff

import "sync"

const (
	SUBSCRIBE_SERVICE_METHOD   = "QueueManager.Subscribe"
	UNSUBSCRIBE_SERVICE_METHOD = "QueueManager.Unsubscribe"
)

type BrokerInfo struct {
	//ip and port are going to be read and used by the consumerStuff
	//to consume brokerStuff messages
	Ip   string
	Port string
}

var BrokerList []BrokerInfo

var ConsumedMessages []string
var Mutex sync.Mutex

var MyIp, Port string
