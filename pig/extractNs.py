#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext



if len(sys.argv) <= 1:
    print "at least 1 arg required (input .nt file). optional arg: output file"
    sys.exit(1)


inputFile = sys.argv[1]
outputDir = dirname(inputFile)
outputFile = "%s/namespaces" % (outputDir)
if (len(sys.argv) == 3):
    outputFile = sys.argv[2]
    
pigScript = """
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
graph = LOAD '$inputFile' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);

resources = FOREACH graph GENERATE FLATTEN(TOBAG(*));


namespaces = FOREACH resources {
	---if url has no slashes or hashtag other than
	namespace = (LAST_INDEX_OF($0, '/', 0) > 8 || LAST_INDEX_OF($0, '#', 0) > 8 ? REGEX_EXTRACT ($0, '<(.*)[#/].*>', 1) AS namespace: $0);
	GENERATE namespace;
}

groupedNamespaces = GROUP namespaces BY $0;

namespaceCounts = FOREACH groupedNamespaces GENERATE group, COUNT(namespaces);
rmf $outputFile
STORE namespaceCounts INTO '$outputFile' USING PigStorage();
"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()
