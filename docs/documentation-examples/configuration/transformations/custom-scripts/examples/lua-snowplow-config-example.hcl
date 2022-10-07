transform {
    use "lua" {
      source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkKCS0tIGlucHV0IGlzIGEgbHVhIHRhYmxlCglsb2NhbCBzcERhdGEgPSBpbnB1dFsiRGF0YSJdCglpZiBzcERhdGFbInBsYXRmb3JtIl0gfj0gIndlYiIgdGhlbgoJICAgcmV0dXJuIHsgRmlsdGVyT3V0ID0gdHJ1ZSB9OwoJZW5kCgoJaWYgc3BEYXRhWyJ1c2VyX2lkIl0gfj0gbmlsIHRoZW4KCQlzcERhdGFbInVpZCJdID0gc3BEYXRhWyJ1c2VyX2lkIl0KCWVsc2UKCQlzcERhdGFbInVpZCJdID0gc3BEYXRhWyJkb21haW5fdXNlcmlkIl0KCWVuZAoJcmV0dXJuICB7IERhdGEgPSBzcERhdGEsIFBhcnRpdGlvbktleSA9IGFwcF9pZCB9CmVuZA=="
  
      snowplow_mode = true # Snowplow mode enabled - this transforms the tsv to a lua table
    }
  }