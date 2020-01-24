all:
	go build -o b broker.go
	go build -o p producer.go
	go build -o c consumer.go