target {
  use "http" {

    // by default no custom invalid/retryable rules so we use current default retrying strategy for target failures in `sourceWriteFunc` (in cli.go) 
    response_handler {
      success = [{ http_status: ["2**"]}] 
      invalid/badrow = []
      retryable = [] 

      retry_strategies {
        transient {
          policy: "exponential"
          max_attempts: 5
          delay: "2 seconds"
        }
        setup {
          policy: "exponential" 
          max_attempts: 10000 //very high value making it basically unlimited
          delay: "30 seconds"
        }
      }
    }
  }
}
