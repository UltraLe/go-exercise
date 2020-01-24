package consumerStuff

type Consumer int

func (c *Consumer) Consume(mess string, reply *string) error {

	Mutex.Lock()
	ConsumedMessages = append(ConsumedMessages, mess)
	Mutex.Unlock()

	*reply = "ACK"

	return nil
}
