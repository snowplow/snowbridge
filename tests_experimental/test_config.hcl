 target {
   use "http" {
     url = "http://localhost:10000"
   }
 }

source {
  use "inMemory" {}
}

license {
  accept = true
}
