# Monitoring Configuration

## Stats and metrics

Stream Replicator comes with configurable logging, [pprof](https://github.com/google/pprof) profiling, [statsD](https://www.datadoghq.com/statsd-monitoring) statistics and [Sentry](https://sentry.io/welcome/) integrations to ensure that you know what’s going on.

### Logging

Use the log_level parameter to specify the log level.

// TODO: Tests and templates for all of this

```hcl
// log level configuration (default: "info")
log_level = "debug"
```

### Sentry Configuration

```hcl
sentry {
  # The DSN to send Sentry alerts to
  dsn   = "https://1234d@sentry.acme.net/28"

  # Whether to put Sentry into debug mode (default: false)
  debug = true

  # Escaped JSON string with tags to send to Sentry (default: "{}")
  tags  = "{\"aKey\":\"aValue\"}"
}
```
### StatsD stats reciever 

```hcl
stats_receiver {
  use "statsd" {
    # StatsD server address
    address = "127.0.0.1:8125"

    # StatsD metric prefix (default: "snowplow.stream-replicator")
    prefix  = "snowplow.stream-replicator"

    # Escaped JSON string with tags to send to StatsD (default: "{}")
    tags    = "{\"aKey\": \"aValue\"}"
  }

  # Time (seconds) the observer waits for new results (default: 1)
  timeout_sec = 2

  # Aggregation time window (seconds) for metrics being collected (default: 15)
  buffer_sec  = 20
}
```

### Profiling

To facilitate debugging performance issues and memory leaks you can turn on profiling mode for the standalone CLI binary like so:

```bash
./stream-replicator -p
```

This will enable a profiling web-server endpoint at `http://localhost:8080/` which you can then use to pull down information on the running application which can then be analysed with `pprof`.

For example:

```bash
host$ curl http://localhost:8080/debug/pprof/heap > heap.out
host$ go tool pprof heap.out
Type: inuse_space
Time: Jan 8, 2021 at 9:42am (CET)
Entering interactive mode (type "help" for commands, "o" for options)
(pprof) top
Showing nodes accounting for 8197.61kB, 100% of 8197.61kB total
Showing top 10 nodes out of 45
      flat  flat%   sum%        cum   cum%
 3072.56kB 37.48% 37.48%  4608.63kB 56.22%  github.com/aws/aws-sdk-go/private/protocol/xml/xmlutil.XMLToStruct
 1536.07kB 18.74% 56.22%  1536.07kB 18.74%  github.com/aws/aws-sdk-go/private/protocol/xml/xmlutil.(*XMLNode).findNamespaces
 1024.16kB 12.49% 68.71%  1024.16kB 12.49%  github.com/aws/aws-sdk-go/aws/endpoints.init
  514.63kB  6.28% 74.99%   514.63kB  6.28%  bytes.makeSlice
     514kB  6.27% 81.26%      514kB  6.27%  bufio.NewReaderSize
  512.10kB  6.25% 87.51%   512.10kB  6.25%  fmt.glob..func1
  512.07kB  6.25% 93.75%   512.07kB  6.25%  net/url.parse
  512.02kB  6.25%   100%   512.02kB  6.25%  crypto/tls.(*Conn).makeClientHello
         0     0%   100%   514.63kB  6.28%  bytes.(*Buffer).Write
         0     0%   100%   514.63kB  6.28%  bytes.(*Buffer).grow
(pprof)
```

These articles are quite useful in digging into how to leverage `pprof`:

- https://blog.detectify.com/2019/09/05/how-we-tracked-down-a-memory-leak-in-one-of-our-go-microservices/
- https://www.freecodecamp.org/news/how-i-investigated-memory-leaks-in-go-using-pprof-on-a-large-codebase-4bec4325e192/