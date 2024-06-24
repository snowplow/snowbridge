target {
  use "http" {

    response_handler {

     // everything is fine, we can ack our data from source.
     // Can such configuration be useful or it's overkill and we should commit to 2** all the time?
      success = [
        //we can have concrete status or wildcard using *
        { http_status: ["2**"]}
      ] 

      // data is for some reason not accepted by target, there is no point of retrying. Just send it to bad/failed target and unblock processing.
      invalid/badrow = [

        //first rule...
        { http_status: ["41*"]},

        //second rule...
        { 
          http_status: ["499", "424"],

          //not only http status but check if response body matches. Extract some part of body (e.g. error code) and check if it's equal/not equal to value.
          body {
            path: "some jq here??"
            equal/not_equal: "some value"
          }
        }
      ]

      // For these retryable errors (assuming we have health check):
      // - set app status as unhealthy for each failed retry
      // - set app status as healthy after successful retry
      // - if we run out of attempts =>  exit/crash/restart
      // - if we don't have max attempts (e.g. like setup errors), we don't get stuck retrying indefinitely, because we are protected by health check. If app is unhealthy for extended period time, it will be killed eventually by kubernetes. 
      retryable = [
        { http_status: ["503"], strategy: "transient"}, 
        { 
          http_status: ["403"],
          strategy: "setup",
          alert: "This is alert sent to ops1. You can configure message for specific error code"
        }
      ]

       // and the list of retry strategies, you can define how many you want and then reference them in your 'retryable' rules
       retry_strategies {
          transient { 
            policy: "exponential"
            max_attempts: 5
            delay: "2 seconds"
          }
          setup {
            policy: "exponential" 
            max_attempts: 10000
            delay: "30 seconds"
          }
      }
    }
  }
}
