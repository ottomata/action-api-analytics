#!/usr/bin/env bash
# Validate MediaWiki Avro encoded message

#CHANNEL=${1:-ApiAction}
#VERSION=${2:-101453221640}
CHANNEL=${1:?MediaWiki log channel expected (e.g. ApiAction)}
VERSION=${2:?Avro schema version expected (e.g. 101453221640)}

TOPIC="mediawiki_${CHANNEL}"
KAFKA_SERVER=kafka1012
REC="${CHANNEL}.avro"
SCALA_PROG=avrotest.scala
SCHEMAS=/srv/event-schemas/avro/mediawiki

# create a temp file for capturing command output
TEMPFILE=$(mktemp -t $(basename $0).XXXXXX)
trap '{ rm -f "$TEMPFILE"; }' EXIT

# Grab a record from kafka
echo "Reading 1 record from ${TOPIC} via ${KAFKA_SERVER}..."
echo "(this may take a while depending on event volume)"
kafkacat -b ${KAFKA_SERVER} -t ${TOPIC} -c 1 > ${REC}

# Validate the header
[[ -f $SCALA_PROG ]] ||
cat <<EOF >$SCALA_PROG
object AvroTest {
  def main(args: Array[String]) {
    val file = new java.io.FileInputStream(args(0));
    val data = new java.io.DataInputStream(file);
    println("magic = %d".format(data.readByte()));
    println("revid = %d".format(data.readLong()));
  }
}
EOF

echo "Checking binary packet header..."
scala $SCALA_PROG $REC > "$TEMPFILE"
grep 'magic = 0' "$TEMPFILE" || {
  echo >&2 "[ERROR] Expected 'magic = 0' in $REC"
  cat "$TEMPFILE"
  exit 1
}
grep "revid = ${VERSION}" "$TEMPFILE" || {
  echo >&2 "[ERROR] Expected 'revid = ${VERSION}' in $REC"
  cat "$TEMPFILE"
  exit 1
}

# Validate the payload
echo "Validating JSON..."
echo "(java.io.EOFException can be safely ignored)"
dd if=${REC} bs=1 skip=9 |
java -jar avro-tools-1.7.7.jar fragtojson \
    --schema-file ${SCHEMAS}/${CHANNEL}/${VERSION}.avsc -
