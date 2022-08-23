#!/bin/bash

# Get directory of script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "starting" #TODO: REMOVE

# Run spEnrichedToJson case on its own first:

export STREAM_REPLICATOR_CONFIG_FILE="${SCRIPT_DIR}/cases/spEnrichedToJson/config.hcl"

echo $STREAM_REPLICATOR_CONFIG_FILE

cat ${SCRIPT_DIR}/cases/spEnrichedToJson/input.txt | ${SCRIPT_DIR}/stream-replicator > ${SCRIPT_DIR}/cases/spEnrichedToJson/result.txt


# We may want to do things this way, or we may just do the above for every case, then run go test ${SCRIPT_DIR}/... 
go test  ${SCRIPT_DIR}/... -run TestCheckSpEnrichedToJsonResult


# We may also just want one directory of input files, rather than having them all in the different `cases` folders.
# For a quick spike of the tests though, this is grand.