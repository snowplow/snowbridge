# Extended configuration for RabbitMQ as a source (all options)

source {
  use "rabbitmq" {
    # URL of the RabbitMQ cluster to connect into
    cluster_url = "localhost:5672"

    # Authenticated user to login
    username    = "admin"

    # Password for user to login
    password    = "secretpassword"

    # Name of the queue to pull messages from
    queue_name  = "my-rabbitmq-queue"

    # Define to bind the queue to an exchange to retrieve messages from
    exchange_name = "my-exchange"

    # The type of exchange (we default to fanout)
    exchange_type = "fanout"

    # Number of events to process concurrently (default: 50)
    concurrent_writes = 20
  }
}
