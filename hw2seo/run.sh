#!/usr/bin/env bash

set -e
set -o pipefail

Log() {
    echo "[$( date +%c )] $*" >&2
}

OUTDIR=hw2/out
RESFILE=temps.kml
NREDUCERS=15
MINCLICKS=1

Log "BUILDING"
./gradlew jar


Log "REMOVING previous directory"
hadoop fs -rm -r -f $OUTDIR

Log "RUNNING JOB"
hadoop jar ./hw2seo_ver3.jar MainClass \
           -Dseo.minclicks=$MINCLICKS \
           -Dmapreduce.job.reduces=$NREDUCERS \
           /data/hw2/clicks/*/*.gz $OUTDIR

hadoop fs -text "$OUTDIR/out2/part-r*"  > result

Log "SUCCESS. Result in temps.kml"
