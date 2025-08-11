#!/bin/bash
VERSION=2.0.7_$(date +"%m%d")
read -p "Please enter a new version number($VERSION)：" APP_VERSION
if [ -z "$APP_VERSION" ]; then
  APP_VERSION=$VERSION
fi

echo "Clean Project ..."
mvn clean -f pom.xml

echo "Update version ..."
mvn versions:set -DnewVersion=$APP_VERSION -DprocessAllModules=true -DallowSnapshots=true -DgenerateBackupPoms=false
mvn -N versions:update-child-modules
mvn versions:commit