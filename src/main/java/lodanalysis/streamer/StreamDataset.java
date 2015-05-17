package lodanalysis.streamer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import lodanalysis.Entry;
import lodanalysis.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.data2semantics.vault.PatriciaVault;
import org.data2semantics.vault.PatriciaVault.PatriciaNode;
import org.data2semantics.vault.Vault;

import com.google.common.collect.HashMultiset;

public class StreamDataset implements Runnable  {
	public class PredicateCounter {
		int count = 1;//how often does this predicate occur. (initialize with 1)
		//how many literals (objects) does it co-occur with
		int objLiteralCount = 0;
		HashSet<PatriciaNode> distinctObjLiteralCount = new HashSet<PatriciaNode>();
		//how many URIs/bnodes (objects) does it co-occur with
		int objNonLiteralCount = 0;
		HashSet<PatriciaNode> distinctObjNonLiteralCount = new HashSet<PatriciaNode>();
        //how many distinct subjects (URIs or bnodes) does it co-occur with
        HashSet<PatriciaNode> distinctSubCount = new HashSet<PatriciaNode>();
	}

	private File datasetDir;
	private InputStream gzipStream;
	private InputStream fileStream;
	private Reader decoder;
	private BufferedReader reader;
	private Vault<String, PatriciaNode> vault =  new PatriciaVault();
	private boolean isNquadFile = false;
	int tripleCount = 0;
	int uriCount = 0;
	int literalCount = 0;
	private HashMultiset<Set<PatriciaNode>> tripleNsCounts = HashMultiset.create();
	private HashMultiset<PatriciaNode> nsCounts = HashMultiset.create();
	private HashMultiset<PatriciaNode> bnodeCounts = HashMultiset.create();
	private HashMultiset<PatriciaNode> classCounts = HashMultiset.create();
	private HashMultiset<PatriciaNode> subjects = null;
	private HashMultiset<PatriciaNode> objects;
	
    private HashMultiset<PatriciaNode> subUris = HashMultiset.create();
    private HashMultiset<PatriciaNode> subBnodes = HashMultiset.create();
    private HashMultiset<PatriciaNode> predUris = HashMultiset.create();
    private HashMultiset<PatriciaNode> predBnodes = HashMultiset.create();
    private HashMultiset<PatriciaNode> objUris = HashMultiset.create();
    private HashMultiset<PatriciaNode> objBnodes = HashMultiset.create();
    private HashMultiset<PatriciaNode> literals = HashMultiset.create();
	    
	
	private HashMap<PatriciaNode, PredicateCounter> predicateCounts = new HashMap<PatriciaNode, PredicateCounter>();
	private Set<PatriciaNode> distinctUris;
	private Set<PatriciaNode> uriBnodeSet;

	
	private Set<PatriciaNode> distinctLangTags = new HashSet<PatriciaNode>();
	private Set<PatriciaNode> distinctDataTypes = new HashSet<PatriciaNode>();
	private Set<PatriciaNode> distinctDefinedObjects = new HashSet<PatriciaNode>();
	private Set<PatriciaNode> distinctDefinedProperties = new HashSet<PatriciaNode>();
	private DescriptiveStatistics uriLengthStats;
	private DescriptiveStatistics uriSubLengthStats = new DescriptiveStatistics();
	private DescriptiveStatistics uriPredLengthStats = new DescriptiveStatistics();
	private DescriptiveStatistics uriObjLengthStats = new DescriptiveStatistics();
	private DescriptiveStatistics literalLengthStats = new DescriptiveStatistics();
	private Set<PatriciaNode> distinctSos;
	
	
	private NodeWrapper subWrapper = new NodeWrapper(vault);
    private NodeWrapper predWrapper = new NodeWrapper(vault);
    private NodeWrapper objWrapper = new NodeWrapper(vault);
	
	private Entry entry;
	public static void stream(Entry entry, File datasetDir) throws IOException {
		StreamDataset aggr = new StreamDataset(entry, datasetDir);

		aggr.run();
	}
	public StreamDataset(Entry entry, File datasetDir) throws IOException {
		this.entry = entry;
		this.datasetDir = datasetDir;
	}

	private void processDataset() throws IOException {
		try {
			File inputFile = new File(datasetDir, Paths.INPUT_NT_GZ);
			if (!inputFile.exists()) {
			    inputFile = new File(datasetDir, Paths.INPUT_NQ_GZ);
			    isNquadFile = true;
			}
			if (inputFile.exists()) {
				BufferedReader br = getNtripleInputStream(inputFile);
				String line = null;
				boolean somethingRead = false;
				while((line = br.readLine())!= null) {
					somethingRead = true;
					processLine(line);
				}
				if (somethingRead) {
					store();
				} else {
					log("empty input file found in dataset " + datasetDir.getName());
				}
				
				close();
			} else {
				log("no input file found in dataset " + datasetDir.getName());
			}

		} catch (Throwable e) {
			//cancel on ALL exception. I want to know whats going on!
			System.out.println("Exception analyzing " + datasetDir.getName());
			e.printStackTrace();
		}
	}

	private void log(String msg) throws IOException {
		if (entry.isVerbose()) System.out.println(msg);
	}

	private void store() throws IOException {
	    
		String datasetMd5 = datasetDir.getName();
		File datasetOutputDir = new File(entry.getMetricParentDir(), datasetMd5);
		if (!datasetOutputDir.exists()) datasetOutputDir.mkdir();
		
		
		/**
		 * Do some post processing with the counts
		 */
		distinctUris = new HashSet<PatriciaNode>(subUris.elementSet());
		distinctUris.addAll(predUris.elementSet());
		distinctUris.addAll(objUris.elementSet());
		 
		 
		uriBnodeSet = new HashSet<PatriciaNode>(distinctUris);
		uriBnodeSet.addAll(subBnodes.elementSet());
		uriBnodeSet.addAll(predBnodes.elementSet());
		uriBnodeSet.addAll(objBnodes.elementSet());
		
		distinctSos = new HashSet<PatriciaNode>(subUris.elementSet());
		distinctSos.addAll(objUris.elementSet());
		distinctSos.addAll(subBnodes.elementSet());
		distinctSos.addAll(objBnodes.elementSet());
		distinctSos.addAll(literals.elementSet());
		
		subjects = HashMultiset.create();
		subjects.addAll(subUris);
		subjects.addAll(subBnodes);
		objects = HashMultiset.create();
		objects.addAll(objUris);
		objects.addAll(objBnodes);
		objects.addAll(literals);
		
		double[] allUriLengths = ArrayUtils.addAll(uriSubLengthStats.getValues(), uriPredLengthStats.getValues());
		uriLengthStats = new DescriptiveStatistics(ArrayUtils.addAll(allUriLengths, uriObjLengthStats.getValues()));
		
		
		
		//store provenance file
		FileUtils.copyFile(StreamDatasets.PROVENANCE_FILE, new File(datasetOutputDir, ".sysinfo"));
		
		writePatriciaCountsToFile(new File(datasetOutputDir, Paths.NS_COUNTS), nsCounts);
		writePatriciaCountsToFile(new File(datasetOutputDir, Paths.BNODE_COUNTS), bnodeCounts);
		writePredCountersToFile(datasetOutputDir, predicateCounts);
		writePatriciaCountsToFile(new File(datasetOutputDir, Paths.CLASS_COUNTS), classCounts);
		
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_SOS_COUNT), distinctSos.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_DATA_TYPES), distinctDataTypes.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_LANG_TAGS), distinctLangTags.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_LITERALS), literals.elementSet().size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_TRIPLES), tripleCount);
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_SUBJECTS), subjects.elementSet().size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_OBJECTS), objects.elementSet().size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_URIS), distinctUris.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_URIS_SUB), subUris.elementSet().size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_BNODES_SUB), subBnodes.elementSet().size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_URIS_OBJ), objUris.elementSet().size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_BNODES_OBJ), objBnodes.elementSet().size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.ALL_URIS), uriCount);
		writeSingleCountToFile(new File(datasetOutputDir, Paths.ALL_LITERALS), uriCount);
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_BNODES_OBJ), objBnodes.elementSet().size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_DEFINED_CLASSES), distinctDefinedObjects.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_DEFINED_PROPERTIES), distinctDefinedProperties.size());
		
		
		
		/**
		 * Write degree info
		 */
		DescriptiveStatistics stats = new DescriptiveStatistics();
		
		for (PatriciaNode pNode: subjects.elementSet()) stats.addValue(subjects.count(pNode));
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_SUBJECTS), stats.getSum());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.OUTDEGREE_AVG), stats.getMean());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.OUTDEGREE_MEDIAN), stats.getPercentile(50));
		writeSingleCountToFile(new File(datasetOutputDir, Paths.OUTDEGREE_STD), stats.getStandardDeviation());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.OUTDEGREE_MAX), stats.getMax());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.OUTDEGREE_MIN), stats.getMin());
		
		//indegree (media/median/mode/range)
		stats.clear();
		for (PatriciaNode pNode: objects.elementSet()) stats.addValue(objects.count(pNode));
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_OBJECTS), stats.getSum());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.INDEGREE_AVG), stats.getMean());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.INDEGREE_MEDIAN), stats.getPercentile(50));
		writeSingleCountToFile(new File(datasetOutputDir, Paths.INDEGREE_STD), stats.getStandardDeviation());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.INDEGREE_MAX), stats.getMax());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.INDEGREE_MIN), stats.getMin());
		//degree (media/median/mode/range)
		HashMultiset<PatriciaNode> degrees = HashMultiset.create();
		degrees.addAll(objects);
		degrees.addAll(subjects);
		stats.clear();
		for (PatriciaNode pNode: subjects.elementSet()) stats.addValue(degrees.count(pNode));
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DEGREE_AVG), stats.getMean());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DEGREE_MEDIAN), stats.getPercentile(50));
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DEGREE_STD), stats.getStandardDeviation());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DEGREE_MAX), stats.getMax());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DEGREE_MIN), stats.getMin());
		
		writeSingleCountToFile(new File(datasetOutputDir, Paths.LITERAL_LENGTH_AVG), literalLengthStats.getMean());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.LITERAL_LENGTH_MEDIAN), literalLengthStats.getPercentile(50));
		writeSingleCountToFile(new File(datasetOutputDir, Paths.LITERAL_LENGTH_STD), literalLengthStats.getStandardDeviation());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.LITERAL_LENGTH_MAX), literalLengthStats.getMax());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.LITERAL_LENGTH_MIN), literalLengthStats.getMin());
		
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_LENGTH_AVG), uriLengthStats.getMean());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_LENGTH_MEDIAN), uriLengthStats.getPercentile(50));
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_LENGTH_STD), uriLengthStats.getStandardDeviation());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_LENGTH_MAX), uriLengthStats.getMax());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_LENGTH_MIN), uriLengthStats.getMin());
		
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_OBJ_LENGTH_AVG), uriObjLengthStats.getMean());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_OBJ_LENGTH_MEDIAN), uriObjLengthStats.getPercentile(50));
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_OBJ_LENGTH_STD), uriObjLengthStats.getStandardDeviation());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_OBJ_LENGTH_MAX), uriObjLengthStats.getMax());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_OBJ_LENGTH_MIN), uriObjLengthStats.getMin());
		
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_SUB_LENGTH_AVG), uriSubLengthStats.getMean());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_SUB_LENGTH_MEDIAN), uriSubLengthStats.getPercentile(50));
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_SUB_LENGTH_STD), uriSubLengthStats.getStandardDeviation());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_SUB_LENGTH_MAX), uriSubLengthStats.getMax());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_SUB_LENGTH_MIN), uriSubLengthStats.getMin());

		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_PRED_LENGTH_AVG), uriPredLengthStats.getMean());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_PRED_LENGTH_MEDIAN), uriPredLengthStats.getPercentile(50));
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_PRED_LENGTH_STD), uriPredLengthStats.getStandardDeviation());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_PRED_LENGTH_MAX), uriPredLengthStats.getMax());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_PRED_LENGTH_MIN), uriPredLengthStats.getMin());
		
		
        FileOutputStream output = new FileOutputStream(new File(datasetOutputDir, Paths.URI_BNODE_SET));
        Writer resourcesFileFw = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8");
        for (PatriciaNode pNode : uriBnodeSet) {
            resourcesFileFw.write(vault.redeem(pNode) + System.getProperty("line.separator"));
        }
        resourcesFileFw.close();
        output.close();

		//this one is a bit different (key is a set of strings)
		File nsTripleCountsFile = new File(datasetOutputDir, Paths.NS_TRIPLE_COUNTS);
		FileWriter namespaceTripleCountsOutput = new FileWriter(nsTripleCountsFile);
		for (com.google.common.collect.Multiset.Entry<Set<PatriciaNode>> entry: tripleNsCounts.entrySet()) {
			for (PatriciaNode pNode: entry.getElement()) {
				namespaceTripleCountsOutput.write(vault.redeem(pNode) + " ");
			}
			namespaceTripleCountsOutput.write("\t" + entry.getCount() + System.getProperty("line.separator"));
		}
		namespaceTripleCountsOutput.close();
		
		
		
		/**
		 * Finally, store the delta of this run
		 */
//		FileUtils.copyFile(StreamDatasets.PROVENANCE_FILE, new File(nsTripleCountsFile.getAbsolutePath() + ".sysinfo"));
		FileUtils.write(new File(datasetOutputDir, StreamDatasets.DELTA_FILENAME), Integer.toString(StreamDatasets.DELTA_ID));
	}



	/**
	 * get nodes. if it is a uri, remove the < and >. For literals, keep quotes. This makes the number of substring operation later on low, and we can still distinguish between URIs and literals
	 * @param line
	 * @return
	 */
	public static String[] parseStatement(String line, boolean isNquadFile) throws IndexOutOfBoundsException {
		int offset = 1;//remove first <
		String sub = line.substring(offset, line.indexOf("> "));
		offset += sub.length()+3;//remove '> <'
		String pred = line.substring(offset, line.indexOf("> ", offset));
		offset += pred.length() + 2;//remove '> '
		
		
		
		int endIndex = line.lastIndexOf(' '); //remove final ' .';
		boolean objIsUri = false;
		if (line.charAt(offset) == '<') {
		    //a uri
		    objIsUri = true;
		    endIndex--;//remove '>'
			offset++;//remove '<' as well
		}
		String obj = line.substring(offset, endIndex);
		String graph = null;
		
		
		if (isNquadFile) {
		    //there might be a graph specified in this statement
		    if (objIsUri) {
		        int separatorIndex = obj.indexOf(' ');
		        if (separatorIndex > 0) {
		            //ah, this line has graph specified
		            graph = obj.substring(separatorIndex + 2);//remove ' <' of ng ('>' is already removed)
		            obj = obj.substring(0, separatorIndex-1);//remove '>' of obj
		        }
		    } else {
		        if (obj.charAt(obj.length() - 1) == '>' && obj.lastIndexOf(' ') > obj.lastIndexOf('^')) {
		            //ah, this line has graph specified. Tricky condition, because we don't want to confuse datatyped literals: "literal"^^<datatype>
		            int separatorIndex = obj.lastIndexOf(' ');
		            graph = obj.substring(separatorIndex + 2, obj.length() - 1);
		            obj = obj.substring(0, separatorIndex);
		        }
		    }
		    
		    
		}
	    return new String[]{sub, pred, obj, graph};
	}

	private void processLine(String line) {
		String[] nodes;
		try {
			nodes = parseStatement(line, isNquadFile);
		} catch (Exception e) {
			// Invalid triples. In our class it should never happen
			return;
		}
		if (nodes.length >= 3) {
		    subWrapper.init(nodes[0], NodeWrapper.Position.SUB);
			predWrapper.init(nodes[1], NodeWrapper.Position.PRED);
			objWrapper.init(nodes[2], NodeWrapper.Position.OBJ);
			
			
			/**
			 * Some generic counters
			 */
			tripleCount++;
//			subjects.add(subWrapper.ticket);
//			objects.add(objWrapper.ticket);
			PredicateCounter predCounter = null;

			if (!predicateCounts.containsKey(predWrapper.ticket)) {
				predCounter = new PredicateCounter();
				predicateCounts.put(predWrapper.ticket, predCounter);
			} else {
			    predCounter = predicateCounts.get(predWrapper.ticket);
			    predCounter.count++;
			}
			predCounter.distinctSubCount.add(subWrapper.ticket);
			
			
			/**
			 * store ns triples
			 */
			Set<PatriciaNode> tripleNs = new HashSet<PatriciaNode>();
			if (subWrapper.nsTicket != null) tripleNs.add(subWrapper.nsTicket);
			if (predWrapper.nsTicket != null) tripleNs.add(predWrapper.nsTicket);
			if (objWrapper.nsTicket != null) tripleNs.add(objWrapper.nsTicket);
			tripleNsCounts.add(tripleNs);

			/**
			 * store ns counters
			 */
			if (subWrapper.isUri) nsCounts.add(subWrapper.nsTicket);
			if (predWrapper.isUri) nsCounts.add(predWrapper.nsTicket);
			if (objWrapper.isUri) nsCounts.add(objWrapper.nsTicket);

			
			/**
			 * store uniq bnodes
			 */
			if (subWrapper.isBnode) bnodeCounts.add(subWrapper.ticket);
			if (predWrapper.isBnode) bnodeCounts.add(predWrapper.ticket);
			if (objWrapper.isBnode) {
			    bnodeCounts.add(objWrapper.ticket);
			    objBnodes.add(objWrapper.ticket);
			}

			
			/**
			 * Store literal info
			 */
			if (objWrapper.isLiteral) {
				literalCount++;
				literalLengthStats.addValue(objWrapper.literalLength);
				literals.add(objWrapper.ticket);
				if (objWrapper.datatype != null) {
					distinctDataTypes.add(objWrapper.datatype);
				}
				if (objWrapper.langTag != null) {
					distinctLangTags.add(objWrapper.langTag);
				}
				predCounter.objLiteralCount++;
				predCounter.distinctObjLiteralCount.add(objWrapper.ticket);
			} else {
				predCounter.objNonLiteralCount++;
				predCounter.distinctObjNonLiteralCount.add(objWrapper.ticket);
			}
			
			/**
			 * Store classes and props
			 */
			if (predWrapper.isRdf_type) {
				classCounts.add(objWrapper.ticket);
				
				if (objWrapper.isDefinedClass()) {
					distinctDefinedObjects.add(subWrapper.ticket);
				} else if (objWrapper.isDefinedProperty()) {
					distinctDefinedProperties.add(subWrapper.ticket);
				}
			}
			
			
			
			
			//Store URI info
			if (subWrapper.isUri) {
				uriCount++;
//				uriLengthStats.addValue(subWrapper.uriLength);
				uriSubLengthStats.addValue(subWrapper.uriLength);
				subUris.add(subWrapper.ticket);
			} else if (subWrapper.isBnode) {
				subBnodes.add(subWrapper.ticket);
			}
			
			
			
			if (predWrapper.isUri) {
				uriCount++;
//				uriLengthStats.addValue(predWrapper.uriLength);
				uriPredLengthStats.addValue(predWrapper.uriLength);
			} else if (predWrapper.isBnode) {
			    //shouldnt be there, but use just in case
			    predBnodes.add(predWrapper.ticket);
			}
			
			
			
			if (objWrapper.isUri) {
				uriCount++;
//				uriLengthStats.addValue(objWrapper.uriLength);
				uriObjLengthStats.addValue(objWrapper.uriLength);
				objUris.add(objWrapper.ticket);
			} else if (objWrapper.isBnode) {
			    objBnodes.add(objWrapper.ticket);
			}
		} else {
			System.out.println("Could not get triple from line. " + Arrays.toString(nodes));
		}

	}
	/**
	 * just a simple helper method, to store the maps with a string as key, and counter as val
	 * @throws IOException 
	 */
	private void writePatriciaCountsToFile(File targetFile, HashMultiset<PatriciaNode> multiset) throws IOException {
		FileWriter fw = new FileWriter(targetFile);
		for (com.google.common.collect.Multiset.Entry<PatriciaNode> entry: multiset.entrySet()) {
			fw.write(vault.redeem((PatriciaNode)entry.getElement()) + "\t" + entry.getCount() + System.getProperty("line.separator"));
			
		}
		fw.close();
		
	}

	private void writePredCountersToFile(File targetDir, HashMap<PatriciaNode, PredicateCounter> predCounters) throws IOException {
		File predCountsFile = new File(targetDir, Paths.PREDICATE_COUNTS);
		FileWriter fwPredCounts = new FileWriter(predCountsFile);
		File predLitCountFiles = new File(targetDir, Paths.PREDICATE_LITERAL_COUNTS);
		FileWriter fwPredLitCounts = new FileWriter(predLitCountFiles);
		File predUriCountsFile = new File(targetDir, Paths.PREDICATE_NON_LIT_COUNTS);
		FileWriter fwPredNonLitCounts = new FileWriter(predUriCountsFile);
		File predSubCountsFile = new File(targetDir, Paths.PREDICATE_SUB_COUNTS);
		FileWriter fwPredSubCountsFile = new FileWriter(predSubCountsFile);
		for (java.util.Map.Entry<PatriciaNode, PredicateCounter> entry: predCounters.entrySet()) {
			String pred = vault.redeem(entry.getKey());
			fwPredCounts.write(pred + "\t" + entry.getValue().count + System.getProperty("line.separator"));
			fwPredLitCounts.write(pred + "\t" + entry.getValue().objLiteralCount + "\t" + entry.getValue().distinctObjLiteralCount.size() + "\t" + System.getProperty("line.separator"));
			fwPredNonLitCounts.write(pred + "\t" + entry.getValue().objNonLiteralCount + "\t" + entry.getValue().distinctObjNonLiteralCount.size() + "\t" + System.getProperty("line.separator"));
			fwPredSubCountsFile.write(pred + "\t" + entry.getValue().distinctSubCount.size()  + System.getProperty("line.separator"));
		}
		fwPredCounts.close();
		fwPredLitCounts.close();
		fwPredNonLitCounts.close();
		fwPredSubCountsFile.close();
	}
	private void writeSingleCountToFile(File targetFile, int val) throws IOException {
		FileUtils.writeStringToFile(targetFile, Integer.toString(val));
	}
	private void writeSingleCountToFile(File targetFile, double val) throws IOException {
		FileUtils.writeStringToFile(targetFile, Double.toString(val));
	}


	private BufferedReader getNtripleInputStream(File file) throws IOException {
		reader = null;
		if (file.getName().endsWith(".gz")) {
			fileStream = new FileInputStream(file);
			gzipStream = new GZIPInputStream(fileStream, 200536);//maximize buffer: http://java-performance.com/
			decoder = new InputStreamReader(gzipStream, "UTF-8");
			reader = new BufferedReader(decoder);
		} else {
			reader = new BufferedReader(new FileReader(file));
		}

		return reader;
	}

	private void close() throws IOException {
		if (gzipStream != null) gzipStream.close();
		if (fileStream != null) fileStream.close();
		if (decoder != null) decoder.close();
		if (reader != null) reader.close();

	}


	/**
	 * del delta. This way, when we re-run (forced) a dataset analysis, but we stop in the middle, we know we have to re-run this dataset again later on
	 * @throws IOException
	 */
	private void delDelta() throws IOException {
		File deltaFile = new File(datasetDir, StreamDatasets.DELTA_FILENAME);
		if (deltaFile.exists()) deltaFile.delete();
	}

	@Override
	public void run() {
		try {
			log("aggregating " + datasetDir.getName());
			delDelta();
			processDataset();
			StreamDatasets.PROCESSED_COUNT++;
			StreamDatasets.printProgress(datasetDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
