#!/bin/bash

jarFile='target/lodAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar'
force='-force';

[ -z "$1" ] && echo "No metric directory provided as argument" && exit 1;

java -jar $jarFile $force -metrics $1 "lodanalysis.model.CreateModels";
