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

###
### You might want to override some of these...
###
### Which you can do by calling this script with them set, e.g.
###    $ NAMENODE_HOST=host1 HDFS_USER=foobar quickstart.sh        # (For Non-HA clusters)
###    $ NAMENODE_CONNECT=<nn-uri> HDFS_USER=foobar quickstart.sh  # (For HA clusters)
### and so on
###

### Check whether the command timeout is present or not.
bash -c 'type timeout >& /dev/null'
IS_TIMEOUT_PRESENT=$?

export NAMENODE_HOST=${NAMENODE_HOST:=`hostname`}
export NAMENODE_PORT=${NAMENODE_PORT:="8020"}

if [ -z $NAMENODE_CONNECT ]; then
    # Run the sanity checks only if the timeout command is present in $PATH
    if [ $IS_TIMEOUT_PRESENT == 0 ]; then
        # check the traditional namenode is accessible
        timeout 1 bash -c 'cat < /dev/null > /dev/tcp/$NAMENODE_HOST/$NAMENODE_PORT' >& /dev/null
        if [ $? != 0 ]; then
            echo "Unable to verify Namenode at $NAMENODE_HOST:$NAMENODE_PORT"
            exit 1
        fi
    fi
fi

export NAMENODE_CONNECT=${NAMENODE_CONNECT:=${NAMENODE_HOST}:${NAMENODE_PORT}}

export ZOOKEEPER_HOST=${ZOOKEEPER_HOST:=`hostname`}
export ZOOKEEPER_PORT=${ZOOKEEPER_PORT:="2181"}
export ZOOKEEPER_ROOT=${ZOOKEEPER_ROOT:="/solr"}

if [ -z $ZOOKEEPER_ENSEMBLE ]; then
    # Run the sanity checks only if the timeout command is present in $PATH
    if [ $IS_TIMEOUT_PRESENT == 0 ]; then
        # check that zookeeper is accessible
        timeout 1 bash -c 'cat < /dev/null > /dev/tcp/$ZOOKEEPER_HOST/$ZOOKEEPER_PORT' >& /dev/null
        if [ $? != 0 ]; then
            echo "Unable to access ZooKeeper at ${ZOOKEEPER_HOST}:${ZOOKEEPER_PORT}$ZOOKEEPER_ROOT"
            exit 1
        fi
    fi
fi

export ZOOKEEPER_ENSEMBLE=${ZOOKEEPER_ENSEMBLE:=${ZOOKEEPER_HOST}:${ZOOKEEPER_PORT}${ZOOKEEPER_ROOT}}

export HDFS_USER=${HDFS_USER:="${USER}"}

# save this off for better error reporting
export USER_SOLR_HOME=${SOLR_HOME}
# where do the Solr binaries live on this host
export SOLR_HOME=${SOLR_HOME:="/opt/cloudera/parcels/CDH/lib/solr"}

QUICKSTART_SCRIPT="${BASH_SOURCE-$0}"
# find the quickstart directory and make sure it's absolute
export QUICKSTART_SCRIPT_DIR=${QUICKSTART_SCRIPT_DIR:=$(dirname "${QUICKSTART_SCRIPT}")}
QUICKSTART_SCRIPT_DIR=$(cd $QUICKSTART_SCRIPT_DIR; pwd)

###
### But probably not what's after here...
###
export ENRON_URL=${ENRON_URL:="http://download.srv.cs.cmu.edu/~enron/enron_mail_20110402.tgz"}

export HDFS_USER_HOME=hdfs://${NAMENODE_CONNECT}/user/${HDFS_USER}
export HDFS_ENRON_INDIR=${HDFS_USER_HOME}/enron/indir
export HDFS_ENRON_OUTDIR=${HDFS_USER_HOME}/enron/outdir

die() { echo "$@" 1>&2 ; exit 1; }

# check solr home can be found locally
if [ ${USER_SOLR_HOME+x} ] && [ ${USER_SOLR_HOME} ]; then
    if [ ! -d $SOLR_HOME ]; then
	echo "Unable to find SOLR_HOME provided by user as $SOLR_HOME, exiting"
	exit 1
    fi
    echo "Found SOLR_HOME $SOLR_HOME as provided by user, continuing"
else
    if [ ! -d $SOLR_HOME ]; then
	echo "Unable to find SOLR_HOME at $SOLR_HOME (typical location for parcel based install)"
	SOLR_HOME="/usr/lib/solr"
	echo "Trying $SOLR_HOME (typical location for package based install)"
	if [ ! -d $SOLR_HOME ]; then
	    echo "Unable to find SOLR_HOME, exiting"
	    exit 1
	fi
	echo Found $SOLR_HOME, continuing
    fi
fi

# check that curl is installed
which curl >& /dev/null
if [ $? != 0 ]; then
    echo Unable to find 'curl' in the PATH, please ensure curl is installed and on the PATH
    exit 1
fi

# working directory for the quickstart
export QUICKSTART_WORKINGDIR=${QUICKSTART_WORKINGDIR:=${HOME}/quickstart_workingdir}
mkdir -p ${QUICKSTART_WORKINGDIR}

# cache location for enron emails
export ENRON_MAIL_CACHE=${QUICKSTART_WORKINGDIR}/enron_email

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

hadoop fs -mkdir -p ${HDFS_ENRON_INDIR} || die "Unable to create ${HDFS_ENRON_INDIR}"

echo Copying enron files from `pwd`/maildir/arora-h to ${HDFS_ENRON_INDIR}/maildir/arora-h. This may take a few minutes.
hadoop fs -mkdir -p ${HDFS_ENRON_INDIR}/maildir/arora-h || die "Unable to create ${HDFS_ENRON_INDIR}"
hdfs dfs -copyFromLocal -f "maildir/arora-h" "${HDFS_ENRON_INDIR}/maildir" || die "Unable to copy enron files from local to HDFS"
echo Copy complete, generating configuration, uploading, and creating SolrCloud collection...

# Generate a template of the instance directory
rm -fr ${QUICKSTART_WORKINGDIR}/emailSearch || die "Unable to remove ${QUICKSTART_WORKINGDIR}/emailSearch"
solrctl --zk ${ZOOKEEPER_ENSEMBLE} instancedir --generate ${QUICKSTART_WORKINGDIR}/emailSearch || die "solrctl instancedir command failed"
cd ${QUICKSTART_WORKINGDIR}/emailSearch/conf || die "Unable to cd to ${QUICKSTART_WORKINGDIR}/emailSearch/conf"

# Usecase specific configuration
rm -f managed-schema
cp $QUICKSTART_SCRIPT_DIR/managed-schema . || die "Unable to access managed-schema"

solrctl --zk ${ZOOKEEPER_ENSEMBLE} collection --delete enron-email-collection >& /dev/null
solrctl --zk ${ZOOKEEPER_ENSEMBLE} instancedir --delete enron-email-collection >& /dev/null

# Upload the configuration to SolrCloud
solrctl --zk ${ZOOKEEPER_ENSEMBLE} instancedir --create enron-email-collection ${QUICKSTART_WORKINGDIR}/emailSearch || die "Unable to create configuration via solrctl"

# Create a Solr collection named enron-email-collection. -s 2 indicates that this collection has two shards.
solrctl --zk ${ZOOKEEPER_ENSEMBLE} collection --create enron-email-collection -s 2 -r 1 -m 2 || die "Unable to create collection"

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
