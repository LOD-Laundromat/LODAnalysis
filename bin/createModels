#!/bin/bash
[ -z $LOD_ANALYSIS_JAR ] && echo "LOD_ANALYSIS_JAR environment variable not set" && exit 1;

force='-force';

[ -z "$1" ] && echo "No metric directory provided as argument" && exit 1;

java -jar $LOD_ANALYSIS_JAR $force -metrics $1 "lodanalysis.model.CreateModels";
