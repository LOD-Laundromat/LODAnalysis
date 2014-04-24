#!/usr/bin/jython
from org.apache.pig.scripting import *
import sys
from os.path import dirname, basename, splitext


@outputSchema("nsConcat:chararray") 
def concatBag(bag):
    #outBag = []
    namespaces = []
    for tuple in bag:
        storeTuple = False
        for word in tuple:
            if word != "":
                namespaces.append(word)
            
    namespaces = set(namespaces)
    sortedNs = sorted(namespaces)
    
    concatNs = "#$#$".join(sortedNs)
        
    return concatNs
pigScript = """
REGISTER d2s4pig/target/d2s4pig-1.0.jar
DEFINE NtLoader com.data2semantics.pig.loaders.NtLoader();
REGISTER datafu/dist/datafu-1.2.1-SNAPSHOT.jar
DEFINE AppendToBag datafu.pig.bags.AppendToBag();
DEFINE distinctUdf org.apache.pig.builtin.Distinct();
graph = LOAD 'test/input.nt' USING NtLoader() AS (sub:chararray, pred:chararray, obj:chararray);

namespaceTriples = FOREACH graph  {
    ---First check if we are dealing with a URI. If we are, extract the namespace. If not, return null;
    sub = (INDEXOF(sub, '<', 0) == 0 ?(LAST_INDEX_OF(sub, '/') > 8 OR LAST_INDEX_OF(sub, '#') > 8 ? REGEX_EXTRACT (sub, '<(.*)[#/].*>', 1): sub) : '');
    pred = (INDEXOF(pred, '<', 0) == 0?(LAST_INDEX_OF(pred, '/') > 8 OR LAST_INDEX_OF(pred, '#') > 8 ? REGEX_EXTRACT (pred, '<(.*)[#/].*>', 1): pred) : '');
    obj = (INDEXOF(obj, '<', 0) == 0?(LAST_INDEX_OF(obj, '/') > 8 OR LAST_INDEX_OF(obj, '#') > 8 ? REGEX_EXTRACT (obj, '<(.*)[#/].*>', 1): obj) : '');   
    
    nsBag = TOBAG(sub, pred, obj);
    GENERATE concatBag(nsBag) as ns;
}
---rmf testoutput
---STORE namespaceTriples INTO 'testoutput' USING PigStorage();


groupedNsTriples = GROUP namespaceTriples BY ns;
groupedNsTriplesCounts = FOREACH groupedNsTriples GENERATE group, COUNT(namespaceTriples);
rmf testoutput
STORE groupedNsTriplesCounts INTO 'testoutput' USING PigStorage();
---dump groupedNsTriples;
"""

if __name__ == '__main__':
    P = Pig.compile(pigScript)
    stats = P.bind().runSingle()
