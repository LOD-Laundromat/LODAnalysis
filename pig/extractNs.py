#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext

inputFile = ""

if len(sys.argv) <= 1:
    print "at least 1 arg required (input .nt file). optional arg: output file"
    sys.exit(1)


inputFile = sys.argv[1]
outputDir = "%s_analysis" % (inputFile)
outputFile = "%s/namespaces" % (outputDir)
if (len(sys.argv) == 3):
    outputFile = sys.argv[2]
    
pigScript = """
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
graph = LOAD '$inputFile' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);

mkdir $outputDir


propNameSpaces = FOREACH graph GENERATE REGEX_EXTRACT (pred, '<(.*)[#/].*>', 1) AS namespace;

propNameSpaces = GROUP propNameSpaces BY namespace;

nameSpaceCounts = FOREACH propNameSpaces GENERATE group, COUNT(propNameSpaces);
rmf $outputFile
STORE nameSpaceCounts INTO '$outputFile' USING PigStorage();
"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()
