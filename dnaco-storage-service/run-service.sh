#!/bin/bash

if [[ `uname` = "MINGW"* ]]; then
	ENGINE_CLASSPATH="target\lib\*;target\*"
else
	ENGINE_CLASSPATH="target/lib/*:target/*"
fi

echo $ENGINE_CLASSPATH

$JAVA_HOME/bin/java -verbosegc -DdevMode=LOCAL -Dname=dnaco-storage-service -Djava.io.tmpdir=temp -Dfile.encoding=UTF-8 -cp $ENGINE_CLASSPATH tech.dnaco.storage.service.Main -conf=config.json $@
