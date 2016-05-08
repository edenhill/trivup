#!/bin/bash
#

#
# Deploy (install) Kafka and Zookeeper (ZooKeeperApp depends on this)
#
# VERSION semantics:
#  if $VERSION is trunk, use Kafka git repo in $KAFKA_DIR
#  else check if download directory exists for $VERSION
#  else download official binary build.
#
# Installs to $DEST_DIR
#

set -e


VERSION=$1
KAFKA_DIR=$2
DEST_DIR=$3


[[ ! -z "$DEST_DIR" ]] || (echo "Usage: $0 VERSION KAFKA_GIT_DIR_OR_EMPTY_STR DEST_DIR" ; exit 1)

LOGFILE=$(mktemp)
echo "### Logfile in $LOGFILE"
echo "# $0 $* at $(date)" > $LOGFILE
echo "# DEST_DIR=$DEST_DIR" >> $LOGFILE


if [[ $VERSION == 'trunk' ]]; then
    echo "### Build from git repo $KAFKA_DIR"

    [[ -z "$CLEAN_BUILD" ]] && CLEAN_BUILD=n

    export PATH=$PATH:$HOME/src/gradle-2.11/bin

    pushd $KAFKA_DIR >> $LOGFILE
    git checkout gradlew gradlew.bat >> $LOGFILE

    echo "### Checking out $VERSION"
    git checkout $VERSION >> $LOGFILE

    # refresh gradle and do clean before rebuilding
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

    # Create a link from DEST_DIR to git tree
    ln -sf "$KAFKA_DIR" "$DEST_DIR"

    echo "### Deployed Kafka $VERSION to $DEST_DIR (symlink to $KAFKA_DIR)"

else

    if [[ ! -f "$DEST_DIR/bin/kafka-server-start.sh" ]]; then
	# Download and install tarball
	mkdir -p "$DEST_DIR"
	URL="http://mirrors.sonic.net/apache/kafka/${VERSION}/kafka_2.10-${VERSION}.tgz"
	echo "### Downloading $VERSION from $URL"
	curl "$URL" | (cd $DEST_DIR && tar xzf - --strip-components=1)
	echo "### Successfully installed $VERSION to $DEST_DIR"
    else
	echo "### $VERSION already installed in $DEST_DIR"
    fi
fi
