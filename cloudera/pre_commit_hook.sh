#!/usr/bin/env bash

set -ex

# activate mvn-gbn wrapper
mv "$(which mvn-gbn-wrapper)" "$(dirname "$(which mvn-gbn-wrapper)")/mvn"

mvn --update-snapshots --batch-mode -Dmaven.test.failure.ignore=true -Dtests.nightly=true --fail-at-end clean install
