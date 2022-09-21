transform {
  use "js" {
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkgewovLyBpbnB1dCBpcyBhbiBvYmplY3QKICAgIHZhciBzcERhdGEgPSBpbnB1dC5EYXRhOwoKICAgIGlmIChzcERhdGFbImFwcF9pZCJdICE9ICJ0ZXN0LWRhdGExIikgewogICAgICAgIHJldHVybiB7CiAgICAgICAgICAgIEZpbHRlck91dDogdHJ1ZQogICAgICAgIH07CiAgICB9CgogICAgcmV0dXJuIGlucHV0Cn0="
    
    snowplow_mode       = true # Snowplow mode enabled - this transforms the tsv to an object on input
  }
}