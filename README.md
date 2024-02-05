# StarflareClient

MQTT 5 Client library.

## Connection

```elixir
# For TLS connections
{:ok, pid} = StarflareClient.connect("mqtt://broker.com")

# For SSL connections
{:ok, pid} = StarflareClient.connect("mqtts://broker.com")

# You can also pass a custom port and customize the connect package
{:ok, pid} = StarflareClient.connect("mqtts://broker.com", port: 8888, keep_alive: 600)
```

## Publishing

```elixir
# Syncronous call
StarflareClient.publish(pid, "topic_name", "payload")

# Asyncronous call, result will be delivered to calling process mailbox
StarflareClient.async_publish(pid, "topic_name", "payload")

# Customizing the publish package
StarflareClient.async_publish(pid, "topic_name", "payload", qos_level: :exactly_once)
```

## Subscribing and unsubscribing

```elixir
# Simple subscription with default options
StarflareClient.subscribe(pid, ["test/+/test"])

# Customizing topic filter flags
StarflareClient.subscribe(pid, [{"test/+/test", [nl: false, qos: :at_most_once]}])

# Unsubscribing
StarflareClient.unsubscribe(pid, ["test/+/test"])
```
