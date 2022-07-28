# rabbitmq target configuration

target {
  use "rabbitmq" {
    cluster_url = "localhost:5672"
    username    = "admin"
    password    = "secretpassword"
    queue_name  = "my-rabbitmq-queue"
  }
}
