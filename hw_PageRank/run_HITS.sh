#!/usr/bin/env bash

set -e
set -o pipefail

Log() {
    echo "[$( date +%c )] $*" >&2
}

WORKDIR=hw_PageRank/HITS
RESFILE=temps.kml
NREDUCERS=15
NITERATION=5

Log "REMOVING previous directory"
hadoop fs -rm -r -f $OUTDIR

Log "RUNNING JOB HITS initialisation"
hadoop jar ./hw_PageRank.jar HitsInitJob \
           -Dmapreduce.job.reduces=$NREDUCERS \
           hw_PageRank/url_graph $WORKDIR/init

Log "RUNNING JOB HITS iterations"
hadoop jar ./hw_PageRank.jar HitsIterJob \
           -Dmapreduce.job.reduces=$NREDUCERS \
           $WORKDIR 1 $NITERATION

Log "RUNNING JOB HITS top obtainer"
hadoop jar ./hw_PageRank.jar HitsTopJob \
           $WORKDIR/iter$NITERATION $WORKDIR/sortedByA a

hadoop jar ./hw_PageRank.jar HitsTopJob \
           $WORKDIR/iter$NITERATION $WORKDIR/sortedByH h

Log "SUCCESS. Result in temps.kml"






