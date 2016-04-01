#!/bin/bash
#

set -e


VERSION=$1
KAFKA_DIR=$2

# refresh gradle and do clean before rebuilding
CLEAN_BUILD=n

export PATH=$PATH:$HOME/src/gradle-2.11/bin

LOGFILE=$(mktemp)
echo "### Logfile in $LOGFILE"
echo "# $0 $* at $(date)" > $LOGFILE

pushd $KAFKA_DIR >> $LOGFILE
git checkout gradlew gradlew.bat >> $LOGFILE

echo "### Checking out $VERSION"
git checkout $VERSION >> $LOGFILE

if [[ $CLEAN_BUILD == y ]]; then
    echo "### Running gradle"
    gradle >> $LOGFILE

    echo "### Cleaning"
    ./gradlew clean >> $LOGFILE
fi

echo "### Building Kafka"
./gradlew jar >> $LOGFILE
popd >> $LOGFILE

echo "### Succesfully built $VERSION"

