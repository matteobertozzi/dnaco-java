#!/bin/bash

GC_FLAGS="-XX:+UseShenandoahGC -Xlog:gc -Xlog:gc+ergo -Xlog:gc+stats"
JAR_PATH=target

$JAVA_HOME/bin/java --enable-preview $GC_FLAGS -DdevMode=LOCAL -Dname=dnaco-storage \
  -Djava.io.tmpdir=temp -Dfile.encoding=UTF-8 \
  -cp $JAR_PATH/*:$JAR_PATH/lib/* tech.dnaco.storage.Main -conf=config.json $@
