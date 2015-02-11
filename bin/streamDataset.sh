#!/bin/bash
jarFile='target/lodAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar'
force='-force';
verbose='-verbose';

[ -z "$1" ] && echo "No dataset provided as argument" && exit 1;
[ -z "$2" ] && echo "No output directory provided to write results to" && exit 1;

java -jar $jarFile $force $verbose -dataset $1 -output $2 "lodanalysis.streamer.StreamDatasets"
