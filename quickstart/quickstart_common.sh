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

###
### But probably not what's after here...
###
export ENRON_URL=${ENRON_URL:="http://download.srv.cs.cmu.edu/~enron/enron_mail_20110402.tgz"}

export HDFS_USER_HOME=hdfs://${NAMENODE_CONNECT}/user/${HDFS_USER}
export HDFS_ENRON_DIR=${HDFS_USER_HOME}/enron
export HDFS_ENRON_INDIR=${HDFS_ENRON_DIR}/indir
export HDFS_ENRON_OUTDIR=${HDFS_ENRON_DIR}/outdir

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
