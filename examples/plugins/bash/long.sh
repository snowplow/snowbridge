#!/usr/bin/env bash

while 
  read input_b64encoded; 
do
  # 1. Decode input 
  input_decoded=$(echo "${input_b64encoded}" | base64 -d)

  # 2. Perform transformation
  output_decoded="Bash Transformation: ${input_decoded}"
  output_b64encoded=$(echo "${output_decoded}" | base64)

  # 3. Echo base64 encoded result back to stdout
  echo "${output_b64encoded}";

  # 4. Echo empty string to stderr
  >&2 echo "";
done
