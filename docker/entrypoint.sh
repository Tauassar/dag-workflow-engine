#!/bin/bash

set -ex

service_name=${SERVICE_NAME//_/-};
service_type=${SERVICE_TYPE};

echo "Trying to run: $service_name $service_type"

$service_name $service_type
