#!/bin/bash

# Get directory of script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

for casepath in ${SCRIPT_DIR}/cases/*/ ; do
    # Iterate through the test cases and run the SR binary for each of them
    echo $casepath
    export STREAM_REPLICATOR_CONFIG_FILE="${casepath}config.hcl"

    cat ${casepath}input.txt | ${SCRIPT_DIR}/stream-replicator > ${casepath}result.txt

    # run test for each case
    go test $casepath
done

# could put go test ${SCRIPT_DIR}/... here too if we wanted to do it that way.

# We may also just want one directory of input files, rather than having them all in the different `cases` folders.
# For a quick spike of the tests though, this is grand.