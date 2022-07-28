# Extended configuration for RabbitMQ as a target (all options)

target {
  use "rabbitmq" {
    # URL of the RabbitMQ cluster to connect into
    cluster_url = "localhost:5672"

    # Authenticated user to login
    username = "admin"

    # Password for user to login
    password = "secretpassword"

    # Can be one of 'exchange' or 'queue'
    # - queue: simple publish to a named queue for 1:1 pub/sub flow
    # - exchange: complex publish so you can leverage 1:N flows (like 'fanout')
    publish_type = "exchange"

    # Name of the queue to push messages to (only needed with publish_type = queue)
    queue_name = "my-rabbitmq-queue"

    # Name of the exchange to push messages onto (only needed with publish_type = exchange)
    exchange_name = "my-exchange"

    # The type of exchange (we default to fanout)
    exchange_type = "fanout"
  }
}
