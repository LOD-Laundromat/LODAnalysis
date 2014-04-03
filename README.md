LODAnalysis
===========

##Requirements
* PIG[1]
* Hadoop[2]
* Python[3]
* D2S4PIG[4]: clone project, run `mvn clean package` to compile, and add it to the PIG classpath


##Analysis
###pipeline:

* Fetch datasets (wouter)
* hadoop fs -put all datasets
* exec `runAnalysis.sh -p <hadoop_path>`
* hadoop fs -get (but merge) hadoop analysis
* java -jar .. <class1> <class2>




###Main Analysis method
Run `runAnalysis.sh` to get more information on how to run all analysis methods
###NameSpace extraction
To run, execute
`pig LODAnalysis/pig/extractNs.py <hadoop_input_file>`. Output is stored in path `<hadoop_input_file>_analysis/namespaces`.
For now, this script only counts the namespaces occuring in predicate position. (we can easily change this)

###Schema Information Extracttion
To compile, run

  ant

in mapReduce directory.

To run the program, run

  hadoop jar lib/datasetAnalysisTools.jar jobs.GetSchemaStats <input dir> <output dir> [--reducetasks "number of reducers"]

please note that "input dir" and "output dir" are supposed to be on the hadoop filesystem.

##Links
1. [http://pig.apache.org/](http://pig.apache.org/)
2. [http://hadoop.apache.org/](http://hadoop.apache.org/)
3. [http://www.python.org/](http://www.python.org/)
4. [https://github.com/Data2Semantics/D2S4Pig](https://github.com/Data2Semantics/D2S4Pig)
