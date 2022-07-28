# Simple configuration for RabbitMQ as a target (only required options)

target {
  use "rabbitmq" {
    # URL of the RabbitMQ cluster to connect into
    cluster_url = "localhost:5672"

    # Authenticated user to login
    username    = "admin"

    # Password for user to login
    password    = "secretpassword"

    # Name of the queue to push messages to
    queue_name  = "my-rabbitmq-queue"
  }
}
