#!/usr/bin/env bash
mvn clean;
mvn package -Dmaven.test.skip=true;
jar uf target/seaweedfs-hadoop2-client-1.0.0.jar core-site.xml;