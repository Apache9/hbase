#!/bin/bash
JAVA_VERSION=`java -version 2>&1 | grep version | cut -d\" -f2`
echo $JAVA_VERSION | grep -qE "^1.8"
if [ "$?" != "0" ]; then
  echo "wrong java version $JAVA_VERSION, expected 1.8"
  exit 1
fi
mvn -DskipTests clean install && mvn -DskipTests package assembly:single
