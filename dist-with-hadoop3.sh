#!/bin/bash
mvn clean install -DskipTests -Dhadoop.profile=3.0 && mvn package assembly:single -DskipTests -Dhadoop.profile=3.0
