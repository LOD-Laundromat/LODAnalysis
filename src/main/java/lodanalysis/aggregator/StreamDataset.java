package lodanalysis.aggregator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import lodanalysis.Entry;
import lodanalysis.Paths;
import lodanalysis.utils.NodeContainer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.data2semantics.vault.PatriciaVault;
import org.data2semantics.vault.PatriciaVault.PatriciaNode;
import org.data2semantics.vault.Vault;

import com.google.common.collect.HashMultiset;

public class StreamDataset implements Runnable  {
	public class PredicateCounter {
		int count = 1;//how often does this predicate occur. (initialize with 1)
		int hasLiteralCount = 0;//how many literals does it co-occur with
		int hasNonLiteralCount = 0;//how many uris does it co-occur with
		HashSet<PatriciaNode> distinctLiteralCount = new HashSet<PatriciaNode>();
		HashSet<PatriciaNode> distinctNonLiteralCount = new HashSet<PatriciaNode>();
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
	private HashMultiset<PatriciaNode> outdegreeCounts = HashMultiset.create();
	private HashMultiset<PatriciaNode> indegreeCounts = HashMultiset.create();
	private HashMap<PatriciaNode, PredicateCounter> predicateCounts = new HashMap<PatriciaNode, PredicateCounter>();
	private Set<PatriciaNode> distinctUris = new HashSet<PatriciaNode>();
	private Set<PatriciaNode> distinctLiterals = new HashSet<PatriciaNode>();
	private HashSet<PatriciaNode> distinctSubUris = new HashSet<PatriciaNode>();
	private HashSet<PatriciaNode> distinctSubBnodes = new HashSet<PatriciaNode>();
	private HashSet<PatriciaNode> distinctObjUris = new HashSet<PatriciaNode>();
	private HashSet<PatriciaNode> distinctObjBnodes = new HashSet<PatriciaNode>();
	private Set<PatriciaNode> distinctLangTags = new HashSet<PatriciaNode>();
	private Set<PatriciaNode> distinctDataTypes = new HashSet<PatriciaNode>();
	private Set<PatriciaNode> distinctDefinedObjects = new HashSet<PatriciaNode>();
	private Set<PatriciaNode> distinctDefinedProperties = new HashSet<PatriciaNode>();
	private DescriptiveStatistics uriLengthStats = new DescriptiveStatistics();
	private DescriptiveStatistics uriSubLengthStats = new DescriptiveStatistics();
	private DescriptiveStatistics uriPredLengthStats = new DescriptiveStatistics();
	private DescriptiveStatistics uriObjLengthStats = new DescriptiveStatistics();
	private DescriptiveStatistics literalLengthStats = new DescriptiveStatistics();
	
	
	
	
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
			//skip files modified in last hour, to avoid concurrency issues
			if (inputFile.exists() && (entry.forceExec() || (new Date().getTime() -  inputFile.lastModified() > 3600000))) {
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
		StreamDatasets.writeToLogFile(msg);

	}

	private void store() throws IOException {
		String datasetMd5 = datasetDir.getName();
		File datasetOutputDir = new File(entry.getMetricsDir(), datasetMd5);
		if (!datasetOutputDir.exists()) datasetOutputDir.mkdir();
		writePatriciaCountsToFile(new File(datasetOutputDir, Paths.NS_COUNTS), nsCounts);
		writePatriciaCountsToFile(new File(datasetOutputDir, Paths.BNODE_COUNTS), bnodeCounts);
		writePredCountersToFile(datasetOutputDir, predicateCounts);
		writePatriciaCountsToFile(new File(datasetOutputDir, Paths.CLASS_COUNTS), classCounts);
		
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_DATA_TYPES), distinctDataTypes.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_LANG_TAGS), distinctLangTags.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_LITERALS), distinctLiterals.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_TRIPLES), tripleCount);
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_SUBJECTS), outdegreeCounts.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_OBJECTS), indegreeCounts.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_URIS), distinctUris.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_URIS_SUB), distinctSubUris.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_BNODES_SUB), distinctSubBnodes.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_URIS_OBJ), distinctObjUris.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_BNODES_OBJ), distinctObjBnodes.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.ALL_URIS), uriCount);
		writeSingleCountToFile(new File(datasetOutputDir, Paths.ALL_LITERALS), uriCount);
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_BNODES_OBJ), distinctObjBnodes.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_DEFINED_CLASSES), distinctDefinedObjects.size());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_DEFINED_PROPERTIES), distinctDefinedProperties.size());
		
		
		
		/**
		 * Write degree info
		 */
		DescriptiveStatistics stats = new DescriptiveStatistics();
		
		for (PatriciaNode pNode: outdegreeCounts.elementSet()) stats.addValue(outdegreeCounts.count(pNode));
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_SUBJECTS), stats.getSum());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.OUTDEGREE_AVG), stats.getMean());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.OUTDEGREE_MEDIAN), stats.getPercentile(50));
		writeSingleCountToFile(new File(datasetOutputDir, Paths.OUTDEGREE_STD), stats.getStandardDeviation());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.OUTDEGREE_MAX), stats.getMax());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.OUTDEGREE_MIN), stats.getMin());
		//indegree (media/median/mode/range)
		stats.clear();
		for (PatriciaNode pNode: indegreeCounts.elementSet()) stats.addValue(indegreeCounts.count(pNode));
		writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_OBJECTS), stats.getSum());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.INDEGREE_AVG), stats.getMean());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.INDEGREE_MEDIAN), stats.getPercentile(50));
		writeSingleCountToFile(new File(datasetOutputDir, Paths.INDEGREE_STD), stats.getStandardDeviation());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.INDEGREE_MAX), stats.getMax());
		writeSingleCountToFile(new File(datasetOutputDir, Paths.INDEGREE_MIN), stats.getMin());
		//degree (media/median/mode/range)
		HashMultiset<PatriciaNode> degrees = HashMultiset.create();
		degrees.addAll(indegreeCounts);
		degrees.addAll(outdegreeCounts);
		stats.clear();
		for (PatriciaNode pNode: outdegreeCounts.elementSet()) stats.addValue(degrees.count(pNode));
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
		 * Finally, store the delta of this run and store provenance
		 */
		FileUtils.copyFile(StreamDatasets.PROVENANCE_FILE, new File(nsTripleCountsFile.getAbsolutePath() + ".sysinfo"));
		FileUtils.write(new File(datasetOutputDir, StreamDatasets.DELTA_FILENAME), Integer.toString(StreamDatasets.DELTA_ID));
	}



	/**
	 * get nodes. if it is a uri, remove the < and >. For literals, keep quotes. This makes the number of substring operation later on low, and we can still distinguish between URIs and literals
	 * @param line
	 * @return
	 */
	public static String[] getNodes(String line, boolean isNquadFile) throws IndexOutOfBoundsException {
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
			nodes = getNodes(line, isNquadFile);
		} catch (Exception e) {
			// Invalid triples. In our class it should never happen
			return;
		}
		if (nodes.length == 3) {
			NodeContainer sub = new NodeContainer(vault, nodes[0], NodeContainer.Position.SUB);
			NodeContainer pred = new NodeContainer(vault, nodes[1], NodeContainer.Position.PRED);
			NodeContainer obj = new NodeContainer(vault, nodes[2], NodeContainer.Position.OBJ);
			
			
			/**
			 * Some generic counters
			 */
			tripleCount++;
			outdegreeCounts.add(sub.ticket);
			indegreeCounts.add(obj.ticket);
//			distinctSubjects.add(sub.ticket);
//			distinctObjects.add(obj.ticket);
			PredicateCounter predCounter = null;
			if (!predicateCounts.containsKey(pred.ticket)) {
				predCounter = new PredicateCounter();
				predicateCounts.put(pred.ticket, predCounter);
			} else {
			    predicateCounts.get(pred.ticket).count++;
			}
			
			
			/**
			 * store ns triples
			 */
			Set<PatriciaNode> tripleNs = new HashSet<PatriciaNode>();
			if (sub.nsTicket != null) tripleNs.add(sub.nsTicket);
			if (pred.nsTicket != null) tripleNs.add(pred.nsTicket);
			if (obj.nsTicket != null) tripleNs.add(obj.nsTicket);
			tripleNsCounts.add(tripleNs);

			/**
			 * store ns counters
			 */
			if (sub.isUri) nsCounts.add(sub.nsTicket);
			if (pred.isUri) nsCounts.add(pred.nsTicket);
			if (obj.isUri) nsCounts.add(obj.nsTicket);

			
			/**
			 * store uniq bnodes
			 */
			if (sub.isBnode) bnodeCounts.add(sub.ticket);
			if (pred.isBnode) bnodeCounts.add(pred.ticket);
			if (obj.isBnode) bnodeCounts.add(obj.ticket);


			if (obj.isLiteral) {
				literalCount++;
				distinctLiterals.add(obj.ticket);
				if (obj.datatype != null) {
					distinctDataTypes.add(obj.datatype);
				}
				if (obj.langTag != null) {
					distinctLangTags.add(obj.langTag);
				}
				predCounter.hasLiteralCount++;
				predCounter.distinctLiteralCount.add(obj.ticket);
			} else {
				predCounter.hasNonLiteralCount++;
				predCounter.distinctNonLiteralCount.add(obj.ticket);
			}
			
			/**
			 * Store classes and props
			 */
			if (pred.isRdf_type) {
				classCounts.add(obj.ticket);
				
				if (obj.isDefinedClass()) {
					distinctDefinedObjects.add(sub.ticket);
				} else if (obj.isDefinedProperty()) {
					distinctDefinedProperties.add(sub.ticket);
				}
				
				
			}
			
			
			
			
			//store URI info
			if (sub.isUri) {
				uriCount++;
				uriLengthStats.addValue(sub.uriLength);
				uriSubLengthStats.addValue(sub.uriLength);
				distinctUris.add(sub.ticket);
				distinctSubUris.add(sub.ticket);
			} else if (sub.isBnode) {
				bnodeCounts.add(sub.ticket);
				distinctSubBnodes.add(sub.ticket);
			}
			if (pred.isUri) {
				uriCount++;
				uriLengthStats.addValue(pred.uriLength);
				distinctUris.add(pred.ticket);
				uriPredLengthStats.addValue(pred.uriLength);
			} else if(pred.isBnode) {
				bnodeCounts.add(pred.ticket);
			}
			if (obj.isUri) {
				uriCount++;
				uriLengthStats.addValue(obj.uriLength);
				uriObjLengthStats.addValue(obj.uriLength);
				distinctUris.add(obj.ticket);
				distinctObjUris.add(obj.ticket);
			} else if (obj.isBnode) {
				literalLengthStats.addValue(obj.literalLength);
				bnodeCounts.add(obj.ticket);
				distinctObjBnodes.add(obj.ticket);
			}
		} else {
			System.out.println("Could not get triple from line. " + nodes.toString());
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
		//also store provenance
		FileUtils.copyFile(StreamDatasets.PROVENANCE_FILE, new File(targetFile.getAbsolutePath() + ".sysinfo"));
	}

	private void writePredCountersToFile(File targetDir, HashMap<PatriciaNode, PredicateCounter> predCounters) throws IOException {
		File predCountsFile = new File(targetDir, Paths.PREDICATE_COUNTS);
		FileWriter fwPredCounts = new FileWriter(predCountsFile);
		File predLitCountFiles = new File(targetDir, Paths.PREDICATE_LITERAL_COUNTS);
		FileWriter fwPredLitCounts = new FileWriter(predLitCountFiles);
		File predUriCountsFile = new File(targetDir, Paths.PREDICATE_NON_LIT_COUNTS);
		FileWriter fwPredNonLitCounts = new FileWriter(predUriCountsFile);
		for (java.util.Map.Entry<PatriciaNode, PredicateCounter> entry: predCounters.entrySet()) {
			String pred = vault.redeem(entry.getKey());
			fwPredCounts.write(pred + "\t" + entry.getValue().count + System.getProperty("line.separator"));
			fwPredLitCounts.write(pred + "\t" + entry.getValue().hasLiteralCount + "\t" + entry.getValue().distinctLiteralCount.size() + "\t" + System.getProperty("line.separator"));
			fwPredNonLitCounts.write(pred + "\t" + entry.getValue().hasNonLiteralCount + "\t" + entry.getValue().distinctNonLiteralCount.size() + "\t" + System.getProperty("line.separator"));
		}
		fwPredCounts.close();
		fwPredLitCounts.close();
		fwPredNonLitCounts.close();
		//also store provenance
		FileUtils.copyFile(StreamDatasets.PROVENANCE_FILE, new File(predCountsFile.getAbsolutePath() + ".sysinfo"));
		FileUtils.copyFile(StreamDatasets.PROVENANCE_FILE, new File(predLitCountFiles.getAbsolutePath() + ".sysinfo"));
		FileUtils.copyFile(StreamDatasets.PROVENANCE_FILE, new File(predUriCountsFile.getAbsolutePath() + ".sysinfo"));
	}
	private void writeSingleCountToFile(File targetFile, int val) throws IOException {
		FileUtils.writeStringToFile(targetFile, Integer.toString(val));
		FileUtils.copyFile(StreamDatasets.PROVENANCE_FILE, new File(targetFile.getAbsolutePath() + ".sysinfo"));
	}
	private void writeSingleCountToFile(File targetFile, double val) throws IOException {
		FileUtils.writeStringToFile(targetFile, Double.toString(val));
		FileUtils.copyFile(StreamDatasets.PROVENANCE_FILE, new File(targetFile.getAbsolutePath() + ".sysinfo"));
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
