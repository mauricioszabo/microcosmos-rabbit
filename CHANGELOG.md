# 0.2.0
* Removed deprecated `rabbit/queues`
* Normalized logs to use our schema (meta and payload, as strings)

# 0.1.5
* Add message's queue name into log metadata
* Remove "msg" root key from log metadata
* Mocked environments will serialize and deserialize JSON message.

# 0.1.2
* RPC support for RabbitMQ
* Deprecated mocked queues `rabbit/queues` in favor of `mocks/queues`

# 0.1.1
* Adding multi-route (one exchange routing to multiple queues) option

# 0.1.0
* Split from Microscope
* Added RabbitMQ queue
