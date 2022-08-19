// TODO: This was just dumped here - clean up and fiture out if anything's missing.

# Stats and metrics

Stream Replicator comes with configurable logging, statistics and Sentry integration to ensure that you know whats going on!

## Configuration via file:

```hcl
// log level configuration (default: "info")
log_level = "info"

sentry {
  # The DSN to send Sentry alerts to
  dsn   = "https://acme.com/1"

  # Whether to put Sentry into debug mode (default: false)
  debug = true

  # Escaped JSON string with tags to send to Sentry (default: "{}")
  tags  = "{\"aKey\":\"aValue\"}"
}

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
## Configuration via environment variables

```bash
# logging
export LOG_LEVEL="debug"

# reporting and stats
export SENTRY_DSN="https://acme.com/1"    \
SENTRY_DEBUG=true                         \
SENTRY_TAGS="{\"aKey\":\"aValue\"}"

export STATS_RECEIVER_NAME="statsd"                       \
STATS_RECEIVER_STATSD_ADDRESS="127.0.0.1:8125"            \
STATS_RECEIVER_STATSD_PREFIX="snowplow.stream-replicator" \
STATS_RECEIVER_TIMEOUT_SEC=2                              \
STATS_RECEIVER_BUFFER_SEC=20
```

# Telemetry

Telemetry allows us to track usage data from the Stream Replicator. 

This functionality is enabled by default. 

## Configuration via file:

```hcl
disable_telemetry = false
user_provided_id = "elmer.fudd@acme.com"
```

## Configuration via environment variables

```bash
export DISABLE_TELEMETRY=false         \
USER_PROVIDED_ID="elmer.fudd@acme.com"
```

# Profiling

To assist in debugging performance issues and memory leaks you can turn on profiling mode for the standalone CLI binary like so:

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
