# Telemetry Configuration

Telemetry allows Snowplow (the maintaners of Stream Replicator) to collect limited data about how the app is being used, in order to help us improve the app.

This functionality is enabled by default, but can be disabled with the `disable_telemetry` option.


Privacy is important to Snowplow, as are Open Source principles, and the wishes of the Open Source community. We are committed to transparency about what data we collect, and how we handle this data. To read more about this, [we have publicly published our telemetry principles](https://docs.snowplow.io/docs/open-source-quick-start/what-is-the-quick-start-for-open-source/telemetry-principles/). You can find a definition of what this application collects [in this schema](https://github.com/snowplow/iglu-central/blob/master/schemas/com.snowplowanalytics.oss/oss_context/jsonschema/1-0-1).

## Configuration via file:

Enabling telemetry:

```hcl
# share usage data (heartbeats, application name and version) with Snowplow to improve the application
disable_telemetry = false

# Optionally provide an email address to identify who is using the app.
user_provided_id = "elmer.fudd@acme.com"
```

Disabling telemetry:

```hcl
# Diables telemetry
disable_telemetry = false
```