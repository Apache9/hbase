#!/bin/bash
mvn -DskipTests clean install && mvn -DskipTests package assembly:single
