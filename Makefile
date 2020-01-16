echo "Building..."
go build broker/broker.go
echo "broker built"
go build producer/producer.go
echo "producer built"
go build consumer/consumer.go
echo "consumer built"

