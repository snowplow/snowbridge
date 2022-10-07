# lua configuration
transform {
  use "lua" {

    # The script encoded in Base 64. The script must define a `main` function which takes one argument and returns an object mapping to engineProtocol. (required)   
    source_b64 = "ZnVuY3Rpb24gbWFpbihpbnB1dCkKICByZXR1cm4gaW5wdXQKZW5k"

  }
}