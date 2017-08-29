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
HADOOP_OPTS_EXPORT=""
while test $# != 0 ; do
  case "$1" in
    --jaas)
        JAAS_CONF="--jaas $2"
        export HADOOP_OPTS="-Djava.security.auth.login.config=$2"
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



# Create a directory that the MapReduceBatchIndexer can write results to. Ensure it's empty
hadoop fs -rm -f -skipTrash -r ${HDFS_ENRON_OUTDIR} || die "Unable to remove old outdir"
hadoop fs -mkdir -p  ${HDFS_ENRON_OUTDIR} || die "Unable to create new outdir"

cd ${QUICKSTART_WORKINGDIR} || die "Unable to cd ${QUICKSTART_WORKINGDIR}"

rm -f morphlines.conf
cp $QUICKSTART_SCRIPT_DIR/morphlines.conf . || die "Unable to access morphlines.conf"

echo Starting MapReduceIndexerTool - batch indexing of the enron emails into SolrCloud

# Use MapReduceIndexerTool to index the data and push it live to enron-email-collecton.
hadoop \
  jar \
  ${SOLR_HOME}/contrib/mr/search-mr-*-job.jar org.apache.solr.hadoop.MapReduceIndexerTool \
  -D 'mapreduce.job.user.classpath.first=true' \
  -D 'mapred.child.java.opts=-Xmx500m' \
  --morphline-file ${QUICKSTART_WORKINGDIR}/morphlines.conf \
  --output-dir ${HDFS_ENRON_OUTDIR} \
  --verbose \
  --go-live \
  --zk-host ${ZOOKEEPER_ENSEMBLE} \
  --collection enron-email-collection \
  ${HDFS_ENRON_INDIR}/maildir/arora-h/inbox

echo Completed batch indexing of the enron emails into SolrCloud, open the Hue Search application or Solr admin GUI to query results
