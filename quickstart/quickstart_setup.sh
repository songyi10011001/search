#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Copyright 2014 Cloudera Inc.

QUICKSTART_SCRIPT="${BASH_SOURCE-$0}"
# find the quickstart directory and make sure it's absolute
export QUICKSTART_SCRIPT_DIR=${QUICKSTART_SCRIPT_DIR:=$(dirname "${QUICKSTART_SCRIPT}")}
QUICKSTART_SCRIPT_DIR=$(cd $QUICKSTART_SCRIPT_DIR; pwd)

. "${QUICKSTART_SCRIPT_DIR}/quickstart_common.sh"

###
### You might want to override some of these...
###
### Which you can do by calling this script with them set, e.g.
###    $ NAMENODE_HOST=host1 HDFS_USER=foobar quickstart.sh        # (For Non-HA clusters)
###    $ NAMENODE_CONNECT=<nn-uri> HDFS_USER=foobar quickstart.sh  # (For HA clusters)
### and so on
###

JAAS_CONF=""
while test $# != 0 ; do
  case "$1" in
    --jaas)
        JAAS_CONF="--jaas $2"
        shift 2
        ;;
    --echo)
        set -o xtrace;
        shift 1;
        ;;
    *)
        echo "Wrong argument: $1"
        exit 1;
        ;;
    esac
done


#TODO add check for available storage space

# Loading data
mkdir -p ${ENRON_MAIL_CACHE}
cd ${ENRON_MAIL_CACHE}
echo Downloading enron email archive to `pwd`
curl -C - -O ${ENRON_URL}

if [ -d enron_mail_20110402 ] && [ -e ".untarred_successfully" ]; then
    echo Using existing untarred enron directory at `pwd` called enron_mail_20110402
else
    # there are 520926 records in the archive, takes about 5 minutes to extract
    echo "Decompressing and Untarring enron email archive, this may take a few minutes (especially if you are not using a local disk...)"
    gzip -d enron_mail_20110402.tgz
    tar -xf enron_mail_20110402.tar
    touch ".untarred_successfully"
fi
cd enron_mail_20110402

echo Establishing working direction in ${HDFS_USER_HOME}
sudo -u hdfs hadoop fs -mkdir -p ${HDFS_USER_HOME} || die "Unable to create ${HDFS_USER_HOME} in HDFS"

sudo -u hdfs hadoop fs -chown ${HDFS_USER} ${HDFS_USER_HOME} || die "Unable to chown ${HDFS_USER_HOME} for ${HDFS_USER}"
#This allows quickstart_run.sh to be run as a different user
sudo -u hdfs hadoop fs -chmod 777 ${HDFS_USER_HOME} || die "Unable to chmod 777 to ${HDFS_USER_HOME}"

hadoop fs -mkdir -p ${HDFS_ENRON_INDIR} || die "Unable to create ${HDFS_ENRON_INDIR}"
sudo -u hdfs hadoop fs -chmod 777 ${HDFS_ENRON_DIR} || die "Unable to chmod 777 to ${HDFS_ENRON_DIR}"
sudo -u hdfs hadoop fs -chmod 777 ${HDFS_ENRON_INDIR} || die "Unable to chmod 777 to ${HDFS_ENRON_INDIR}"


echo Copying enron files from `pwd`/maildir/arora-h to ${HDFS_ENRON_INDIR}/maildir/arora-h. This may take a few minutes.
hadoop fs -mkdir -p ${HDFS_ENRON_INDIR}/maildir/arora-h || die "Unable to create ${HDFS_ENRON_INDIR}"
hdfs dfs -copyFromLocal -f "maildir/arora-h" "${HDFS_ENRON_INDIR}/maildir" || die "Unable to copy enron files from local to HDFS"
echo Copy complete, generating configuration, uploading, and creating SolrCloud collection...

# Generate a template of the instance directory
rm -fr ${QUICKSTART_WORKINGDIR}/emailSearch || die "Unable to remove ${QUICKSTART_WORKINGDIR}/emailSearch"
solrctl --zk ${ZOOKEEPER_ENSEMBLE} ${JAAS_CONF} instancedir --generate ${QUICKSTART_WORKINGDIR}/emailSearch || die "solrctl instancedir command failed"
cd ${QUICKSTART_WORKINGDIR}/emailSearch/conf || die "Unable to cd to ${QUICKSTART_WORKINGDIR}/emailSearch/conf"

# Usecase specific configuration
rm -f managed-schema
cp $QUICKSTART_SCRIPT_DIR/managed-schema . || die "Unable to access managed-schema"

solrctl --zk ${ZOOKEEPER_ENSEMBLE} ${JAAS_CONF} collection --delete enron-email-collection >& /dev/null
solrctl --zk ${ZOOKEEPER_ENSEMBLE} ${JAAS_CONF} instancedir --delete enron-email-collection >& /dev/null

# Upload the configuration to SolrCloud
solrctl --zk ${ZOOKEEPER_ENSEMBLE} ${JAAS_CONF} instancedir --create enron-email-collection ${QUICKSTART_WORKINGDIR}/emailSearch || die "Unable to create configuration via solrctl"

# Create a Solr collection named enron-email-collection. -s 2 indicates that this collection has two shards.
solrctl --zk ${ZOOKEEPER_ENSEMBLE} ${JAAS_CONF} collection --create enron-email-collection -s 2 -r 1 -m 2 -c enron-email-collection || die "Unable to create collection"

echo Completed setup, you may run quickstart_run.sh
