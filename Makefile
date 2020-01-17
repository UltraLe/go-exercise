all:
	go build -o b broker/broker.go
	go build -o p producer/producer.go
	go build -o c consumer/consumer.go