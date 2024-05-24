kafka:
	docker run -p 9092:9092 apache/kafka:3.7.0
	bin/kafka-topics.sh --create --topic driver-location --bootstrap-server localhost:9092

run producer:
	go run cmd/producer/main.go
