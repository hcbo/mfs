#!/usr/bin/env bash
mvn clean;
mvn package -Dmaven.test.skip=true;
jar uf target/hcbMfs-1.0.0.jar core-site.xml;