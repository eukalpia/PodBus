# RabbitMQ

`podbus_rabbitmq` is scaffolded. The package defines `RabbitMqMessagingConfig` and an adapter class that fails explicitly for transport operations.

Planned behavior:

- exchange and routing key mapping
- queue declaration and binding
- prefetch
- manual ack/nack
- durable queues
- persistent messages
- dead-letter exchange
- delayed retry strategy where feasible

No RabbitMQ production behavior is claimed yet.
