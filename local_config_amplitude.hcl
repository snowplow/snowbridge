source {
  use "eventHub" {
    # Azure EventHub hub name to read from (required)
    namespace = "mikhail-snowplow-namespace.servicebus.windows.net"
    name      = "enriched-topic"

    # Maximum concurrent goroutines (lightweight threads) for message processing (default: 50)
    concurrent_writes = 1
  }
}

transform {
  use "jq" {
    jq_command = <<JQEOT
{
    "user_id": .user_id,
    "device_id": .domain_userid // "Desktop",
    "event_type": .event_name,
    "session_id": if .contexts_com_snowplowanalytics_snowplow_client_session_1[0].firstEventTimestamp != null then .contexts_com_snowplowanalytics_snowplow_client_session_1[0].firstEventTimestamp | strptime("%Y-%m-%dT%H:%M:%S.%fZ") | mktime * 1000 else null end,
    "insert_id": .event_id,
    "time": .collector_tstamp | epochMillis,
    "event_properties": {
        "page_title": .page_title,
        "page_url": .page_url,
        "screen_name": .unstruct_event_com_snowplowanalytics_mobile_screen_view_1.name
    },
    "$skip_user_properties_sync": false,
    "app_version": .contexts_com_snowplowanalytics_mobile_application_1[0].version,
    "platform": .platform,
    "os_name": .os_name,
    "os_version": .contexts_nl_basjes_yauaa_context_1[0].operatingSystemVersion,
    "device_brand": .contexts_nl_basjes_yauaa_context_1[0].deviceBrand,
    "device_model": .contexts_nl_basjes_yauaa_context_1[0].deviceName,
    "carrier": .contexts_nl_basjes_yauaa_context_1[0].carrier,
    "country": .geo_country,
    "region": .geo_region, 
    "city": .geo_city,
    "language": .br_lang,
    "revenue": .contexts_com_snowplowanalytics_snowplow_ecommerce_transaction_1[0].revenue,
    "productId": .contexts_com_snowplowanalytics_snowplow_ecommerce_product_1[0].id,
    "location_lat": .geo_latitude,
    "location_lng": .geo_longitude,
    "ip": .user_ipaddress,
    "idfa": .contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].appleIdfa, 
    "idfv": .contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].appleIdfv, 
    "adid": .contexts_com_snowplowanalytics_snowplow_mobile_context_1[0].androidIdfa
}
JQEOT

    timeout_ms = 5
    snowplow_mode = true
  }
}

target {
  use "http" { 
    url = "https://api2.amplitude.com/2/httpapi"
    template_file = "./local_config_template_amplitude.tmpl"
  }
}

disable_telemetry = true
log_level = "debug"

license {
  accept = true
}
