#!/bin/bash

mvn clean -f pom.xml
mvn compile package -f pom.xml -Dmaven.test.skip=true

CURRENT_DIR=$(pwd);
cp -rvf $CURRENT_DIR/dbsyncer-web/target/dbsyncer-*.zip $CURRENT_DIR