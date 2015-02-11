#!/bin/bash
jarFile='target/lodAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar'
force='-force';
#new Entry(new String[]{"-force","-nostrict",  "-verbose", "-data_version", "11",  "-threads", "1","-dataset", "testDataset", "-metrics", "metrics", "lodanalysis.streamer.StreamDatasets"});

[ -z "$1" ] && echo "No argument supplied" && exit 1;


for f in $1; do
  java -jar $jarFile $force -
done