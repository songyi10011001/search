#!/usr/bin/env bash

set -ex

# install mvn settings so dependencies come from a GBN of the latest build
export CDH_GBN="$(curl "http://builddb.infra.cloudera.com:8080/query?product=cdh&version=6.x&user=jenkins&tag=official")"
MVN_SETTINGS_FILE="$(mktemp)"
function cleanup {
  rm -f "$MVN_SETTINGS_FILE"
}
trap cleanup EXIT
curl -L "http://github.mtv.cloudera.com/raw/CDH/cdh/cdh6.x/gbn-m2-settings.xml" > "$MVN_SETTINGS_FILE"

mvn -U -s "$MVN_SETTINGS_FILE" clean compile test-compile
