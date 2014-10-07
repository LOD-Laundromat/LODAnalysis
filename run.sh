#!/bin/bash
git pull;
mvn package;
dataVersion=11
threads=6;
memory="-Xmx30G"
#force="-force";
force="";
jar="target/lodAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar"
java $memory -jar $jar  $force -data_version $dataVersion -threads $threads -datasets /scratch/lodlaundromat/11 -metrics /scratch/lodlaundromat/metrics-11  lodanalysis.aggregator.Aggregator;
java -jar $jar $force -data_version $dataVersion -threads $threads -datasets /scratch/lodlaundromat/11 -metrics /scratch/lodlaundromat/metrics-11 lodanalysis.metrics.CreateDescriptions;
java -jar $jar  $force -data_version $dataVersion -threads $threads -datasets /scratch/lodlaundromat/11 -metrics /scratch/lodlaundromat/metrics-11 lodanalysis.metrics.StoreDescriptionsInEndpoint;


