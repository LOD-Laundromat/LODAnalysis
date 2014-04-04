#!/usr/bin/python
from org.apache.pig.scripting import Pig
import sys
from os.path import dirname, basename, splitext



if len(sys.argv) <= 1:
    print "at least 1 arg required (input dataset dir). optional arg: output file"
    sys.exit(1)


inputDir = sys.argv[1]
inputFile = "%s/input.nt" % (inputDir)
namespaceCountsOutput = "%s/namespaceCounts" % (inputDir)
namespaceUniqCountsOutput = "%s/namespaceUniqCounts" % (inputDir)
languageTagCounts = "%s/langTagCounts" % (inputDir)
languageTagCountsWithoutRegion = "%s/langTagCountsWithoutRegion" % (inputDir)
dataTypeCounts = "%s/dataTypeCounts" % (inputDir)
uniqUris = "%s/urisUniq" % (inputDir)
if (len(sys.argv) == 3):
    outputFile = sys.argv[2]
    
pigScript = """
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
graph = LOAD '$inputFile' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);
resources = FOREACH graph GENERATE FLATTEN(TOBAG(*));


objects = FOREACH graph GENERATE obj;
literals = FILTER objects BY SUBSTRING($0, 0, 1) != '<';

---calc data types

dataTypes = FOREACH literals GENERATE REGEX_EXTRACT ($0, '.*"\\\^\\\^<(.*)>$', 1);
groupedDataTypes = GROUP dataTypes BY $0;
dataTypeCounts =  FOREACH groupedDataTypes GENERATE group, COUNT(dataTypes);
rmf $dataTypeCounts
STORE dataTypeCounts INTO '$dataTypeCounts' USING PigStorage();

---calc lang tags
languageTags = FOREACH literals GENERATE REGEX_EXTRACT ($0, '.*"@(.*)\\\s*$', 1);
groupedLanguageTags = GROUP languageTags BY $0;
languageTagCounts =  FOREACH groupedLanguageTags GENERATE group, COUNT(languageTags);
rmf $languageTagCounts
STORE languageTagCounts INTO '$languageTagCounts' USING PigStorage();

---calc lang tags (without regions. e.g. fr-be becomes fr)
languageTagsWithoutReg = FOREACH languageTags GENERATE (INDEXOF($0, '-') > 0? REGEX_EXTRACT ($0, '(.*)-.*$', 1) :$0);
groupedLanguageTagsWithoutReg = GROUP languageTagsWithoutReg BY $0;
languageTagWihtoutRegCounts =  FOREACH groupedLanguageTagsWithoutReg GENERATE group, COUNT(languageTagsWithoutReg);
rmf $languageTagCountsWithoutRegion
STORE languageTagWihtoutRegCounts INTO '$languageTagCountsWithoutRegion' USING PigStorage();


---filter out literals
filteredResources = FILTER resources BY SUBSTRING($0, 0, 1) == '<';



---store unique uris
uniqUris = DISTINCT filteredResources;
rmf $uniqUris
STORE uniqUris INTO '$uniqUris' USING PigStorage();





---calc namespace counts
namespaces = FOREACH filteredResources {
	---if url has no slashes or hashtag other than the one in http://, use complete uri as namespace..
	namespace = (LAST_INDEX_OF($0, '/') > 8 OR LAST_INDEX_OF($0, '#') > 8 ? REGEX_EXTRACT ($0, '<(.*)[#/].*>', 1): $0);
	GENERATE namespace;
}
groupedNamespaces = GROUP namespaces BY $0;
namespaceCounts = FOREACH groupedNamespaces GENERATE group, COUNT(namespaces);
rmf $namespaceCountsOutput
STORE namespaceCounts INTO '$namespaceCountsOutput' USING PigStorage();


---calc namespace counts uniq
namespacesUniq = FOREACH uniqUris {
    ---if url has no slashes or hashtag other than the one in http://, use complete uri as namespace..
    namespace = (LAST_INDEX_OF($0, '/') > 8 OR LAST_INDEX_OF($0, '#') > 8 ? REGEX_EXTRACT ($0, '<(.*)[#/].*>', 1): $0);
    GENERATE namespace;
}
groupedNamespacesUniq = GROUP namespacesUniq BY $0;
namespaceUniqCounts = FOREACH groupedNamespacesUniq GENERATE group, COUNT(namespacesUniq);
rmf $namespaceUniqCountsOutput
STORE namespaceUniqCounts INTO '$namespaceUniqCountsOutput' USING PigStorage();
"""


P = Pig.compile(pigScript)
stats = P.bind().runSingle()