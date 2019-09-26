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
echo "# $0 $* at $(date)" > $LOGFILE
echo "# DEST_DIR=$DEST_DIR" >> $LOGFILE

function kafka_build {
    echo "### $0: Build from git repo $KAFKA_DIR"

    [[ -z "$CLEAN_BUILD" ]] && CLEAN_BUILD=n

    pushd $KAFKA_DIR >> $LOGFILE
    [[ $CLEAN_BUILD == y ]] && git checkout gradlew gradlew.bat >> $LOGFILE

    echo "### $0: Checking out $VERSION"
    git checkout $VERSION >> $LOGFILE

    if [[ $CLEAN_BUILD == y || ! -d .gradle ]]; then
	echo "### $0: Running gradle"
	gradle >> $LOGFILE
    fi

    # refresh gradle and do clean before rebuilding
    if [[ $CLEAN_BUILD == y ]]; then
	echo "### $0: Cleaning"
	./gradlew clean >> $LOGFILE
    fi

    echo "### $0: Building Kafka"
    ./gradlew jar >> $LOGFILE
    popd >> $LOGFILE

    echo "### $0: Succesfully built $VERSION"

    # Create a link from DEST_DIR to git tree
    ln -sf "$KAFKA_DIR" "$DEST_DIR"

    echo "### $0: Deployed Kafka $VERSION to $DEST_DIR (symlink to $KAFKA_DIR)"
}

function kafka_git_clone {
    echo "### $0: Git cloning kafka to $KAFKA_DIR"
    git clone https://github.com/apache/kafka.git "$KAFKA_DIR"
}


if [[ $VERSION == 'trunk' ]]; then
    if [[ ! -f "$KAFKA_DIR/README.md" ]]; then
	# No Kafka sources, check them out.
	kafka_git_clone
    fi

    kafka_build

else

    if [[ ! -f "$DEST_DIR/bin/kafka-server-start.sh" ]]; then
	# Download and install tarball
	mkdir -p "$DEST_DIR"
	if [[ -z "$KAFKA_URL" ]]; then
	    KAFKA_URL="https://archive.apache.org/dist/kafka/${VERSION}/kafka_2.12-${VERSION}.tgz https://archive.apache.org/dist/kafka/${VERSION}/kafka_2.11-${VERSION}.tgz"
	fi
	TRY_URLS="$KAFKA_URL"
	downloaded=0
	for URL in $TRY_URLS; do
	    echo "### $0: Downloading $VERSION from $URL"
	    curl -L -s "$URL" | (cd $DEST_DIR && tar xzf - --strip-components=1) || continue
	    echo "### $0: Successfully installed $VERSION to $DEST_DIR"
	    downloaded=1
            break
	done
	if [[ $downloaded == 0 ]]; then
	   echo "# Download of $VERSION failed"
	   exit 1
	fi
    else
	echo "# $VERSION already installed in $DEST_DIR" >> $LOGFILE
    fi
fi
