package producerStuff

const (
	TCP_STYLE         = 1 //each request is sent every 1s, 2s, 4s, 8s... MAX_SEC
	BRUTE_FORCE_STYLE = 2 //never stop trying to send the message
	MAX_TRIES_STYLE   = 3 //do at most MAX_TRIES_STYLE tries

	PUBLISH_SERVICE_METHOD = "QueueManager.Publish"
	MAX_SEC                = 64
	GOR_EXIT               = 0 //used to request the closure of the "publish" go routine
)

type BrokerInfo struct {

	//ip and port are going to be read and used by the producerStuff
	//to establish a connection with the brokerStuff
	Ip              string
	Port            string
	MessagesChannel chan string
	CtrlChannel     chan int
}

var BrokerList []BrokerInfo

var ServiceMethod string
var RR1Mode int
var MaxTimes int
