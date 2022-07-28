# rabbitmq target configuration

target {
  use "rabbitmq" {
    cluster_url   = "localhost:5672"
    username      = "admin"
    password      = "secretpassword"
    queue_name    = "my-rabbitmq-queue"
    publish_type  = "exchange"
    exchange_name = "my-exchange"
    exchange_type = "fanout"
  }
}
