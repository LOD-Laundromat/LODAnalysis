#!/bin/sh

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <hadoop job name> <input> <output>";
  exit 1;
fi

workingDir=`basename $PWD`;

if [[  $workingDir == "LODAnalysis" ]]; then
  path="mapReduce";
elif [[ $workingDir == "mapReduce" ]]; then
  path=".";
else
  echo "You are in the wrong working dir";
  exit;
fi

hadoop jar $path/lib/datasetAnalysisTools.jar jobs.$1 $2 $3
