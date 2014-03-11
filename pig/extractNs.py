#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext



if len(sys.argv) <= 1:
    print "at least 1 arg required (input dataset dir). optional arg: output file"
    sys.exit(1)


inputDir = sys.argv[1]
inputFile = "%s/input.nt" % (inputDir)
outputFile = "%s/namespaces" % (inputDir)
if (len(sys.argv) == 3):
    outputFile = sys.argv[2]
    
pigScript = """
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
graph = LOAD '$inputFile' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);

resources = FOREACH graph GENERATE FLATTEN(TOBAG(*));

---filter out literals
filteredResources = FILTER resources BY SUBSTRING($0, 0, 1) == '<';

namespaces = FOREACH filteredResources {
	---if url has no slashes or hashtag other than the one in http://, use complete uri as namespace..
	namespace = (LAST_INDEX_OF($0, '/') > 8 OR LAST_INDEX_OF($0, '#') > 8 ? REGEX_EXTRACT ($0, '<(.*)[#/].*>', 1): $0);
	GENERATE namespace;
}

groupedNamespaces = GROUP namespaces BY $0;

namespaceCounts = FOREACH groupedNamespaces GENERATE group, COUNT(namespaces);
rmf $outputFile
STORE namespaceCounts INTO '$outputFile' USING PigStorage();
"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()
