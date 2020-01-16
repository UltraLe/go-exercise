all:
	echo "Building..."
	go build -o b broker/broker.go
	echo "broker built"
	go build -o p producer/producer.go
	echo "producer built"
	go build -o c consumer/consumer.go
	echo "consumer built"