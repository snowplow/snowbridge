#!/bin/bash

# Get directory of script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

for casepath in ${SCRIPT_DIR}/cases/*/ ; do

# TODO: For this quick spike we don't need any external resources. We'd need to figure out where spinning up infrastructure fits. 
# Maybe each case has a script that gets called here, maybe it's something different.


    # Iterate through the test cases and run the SR binary for each of them
    echo $casepath
    # export STREAM_REPLICATOR_CONFIG_FILE="${casepath}config.hcl"

    # cat ${casepath}input.txt | ${SCRIPT_DIR}/stream-replicator > ${casepath}result.txt
    
    # -i keeps stdin open
    # --mount mounts the config
    # --env sets the config env var
    cat ${casepath}input.txt | docker run -i \
        --mount type=bind,source=${casepath}config.hcl,target=/config.hcl \
        --env STREAM_REPLICATOR_CONFIG_FILE=/config.hcl \
        snowplow/stream-replicator-aws:1.0.0 \
    > ${casepath}result.txt # output to file


    # The docker way is something like:
    # cat cases/spEnrichedFilter/input.txt | docker run -i --mount type=bind,source="$(pwd)"/cases/spEnrichedFilter/config.hcl,target=/config.hcl --env STREAM_REPLICATOR_CONFIG_FILE=/config.hcl snowplow/stream-replicator-aws:1.0.0 > result.txt

    # `make container` first
    # Then we just need management of the version number. Could do that with cat VERSION...

    # run test for each case
    go test $casepath
    
done

# TODO:

# For now, running each test as part of this script.. Might be better to do that as its own GH action though.
# Also need to run it once each for gcp and aws