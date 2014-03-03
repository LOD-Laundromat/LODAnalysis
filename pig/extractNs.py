#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext

inputFile = ""

if len(sys.argv) <= 1:
    print "at least 1 arg required (input .nt file). optional arg: output file"
    sys.exit(1)


inputFile = sys.argv[1]

outputFile = "%s/namespaces" % (dirname(inputFile))
if (len(sys.argv) == 3):
    outputFile = sys.argv[2]
    
pigScript = """
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
"""

pigScript += """
graph = LOAD '$inputFile' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);

rmf $outputFile
STORE namespaces INTO '$outputFile' USING PigStorage();
"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()
