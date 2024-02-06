# StarflareClient

MQTT 5 Client library.

## Connection and disconnection

```elixir
# For TLS connections
{:ok, name} = StarflareClient.connect("mqtt://broker.com")

# For SSL connections
{:ok, name} = StarflareClient.connect("mqtts://broker.com")

# You can also pass a custom port and customize the connect packet
{:ok, name} = StarflareClient.connect("mqtts://broker.com", port: 8888, keep_alive: 600)

# Disconnect from broker
StarflareClient.disconnect(name)
```

## Publishing

```elixir
# Syncronous call
StarflareClient.publish(name, "topic_name", "payload")

# Asyncronous call, result will be delivered to calling process mailbox
StarflareClient.async_publish(name, "topic_name", "payload")

# Customizing the publish packet
StarflareClient.async_publish(name, "topic_name", "payload", qos_level: :exactly_once)
```

## Subscribing and unsubscribing

```elixir
# Simple subscription with default options
StarflareClient.subscribe(name, ["test/+/test"])

# Customizing topic filter flags
StarflareClient.subscribe(name, [{"test/+/test", [nl: false, qos: :at_most_once]}])

# Unsubscribing
StarflareClient.unsubscribe(name, ["test/+/test"])
```
