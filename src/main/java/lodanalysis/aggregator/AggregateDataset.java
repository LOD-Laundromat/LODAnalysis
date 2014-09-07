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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import lodanalysis.Entry;
import lodanalysis.Settings;
import lodanalysis.utils.NodeContainer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.data2semantics.vault.PatriciaVault;
import org.data2semantics.vault.PatriciaVault.PatriciaNode;
import org.data2semantics.vault.Vault;

import com.google.common.collect.HashMultiset;

public class AggregateDataset implements Runnable  {
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
	int tripleCount = 0;
	private HashMultiset<Set<PatriciaNode>> tripleNsCounts = HashMultiset.create();
	private HashMultiset<String> dataTypeCounts = HashMultiset.create();
	private HashMultiset<String> langTagCounts = HashMultiset.create();
	private HashMultiset<String> langTagWithoutRegCounts = HashMultiset.create();
	private HashMultiset<PatriciaNode> nsCounts = HashMultiset.create();
	private HashMultiset<PatriciaNode> uniqBnodeCounts = HashMultiset.create();
	private HashMultiset<PatriciaNode> typeCounts = HashMultiset.create();
	private HashMultiset<PatriciaNode> outdegreeCounts = HashMultiset.create();
	private HashMultiset<PatriciaNode> indegreeCounts = HashMultiset.create();
	private HashMap<PatriciaNode, PredicateCounter> predicateCounts = new HashMap<PatriciaNode, PredicateCounter>();
	private Set<PatriciaNode> distinctUris = new HashSet<PatriciaNode>();
	private Set<PatriciaNode> distinctLiterals = new HashSet<PatriciaNode>();
	
	private Entry entry;
	public static void aggregate(Entry entry, File datasetDir) throws IOException {
		AggregateDataset aggr = new AggregateDataset(entry, datasetDir);

		aggr.run();
	}
	public AggregateDataset(Entry entry, File datasetDir) throws IOException {
		this.entry = entry;
		this.datasetDir = datasetDir;
	}

	private void processDataset() throws IOException {
		try {
			File inputFile = new File(datasetDir, Settings.FILE_NAME_INPUT_GZ);
			if (inputFile.exists()) {
				BufferedReader br = getNtripleInputStream(inputFile);
				String line = null;
				while((line = br.readLine())!= null) {
					processLine(line);
				}
				store();
				
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
		Aggregator.writeToLogFile(msg);

	}

	private void store() throws IOException {
		String datasetMd5 = datasetDir.getName();
		File datasetOutputDir = new File(entry.getMetricsDir(), datasetMd5);
		if (!datasetOutputDir.exists()) datasetOutputDir.mkdir();
		writeCountersToFile(new File(datasetOutputDir, Settings.FILE_NAME_NS_COUNTS), nsCounts);
		writeStringCountersToFile(new File(datasetOutputDir, Settings.FILE_NAME_LANG_TAG_COUNTS), langTagCounts);
		writeStringCountersToFile(new File(datasetOutputDir, Settings.FILE_NAME_LANG_TAG_NOREG_COUNTS), langTagWithoutRegCounts);
		writeStringCountersToFile(new File(datasetOutputDir, Settings.FILE_NAME_DATATYPE_COUNTS), dataTypeCounts);
		writeCountersToFile(new File(datasetOutputDir, Settings.FILE_NAME_UNIQ_BNODES_COUNTS), uniqBnodeCounts);
		writePredCountersToFile(datasetOutputDir, predicateCounts);
		writeCountersToFile(new File(datasetOutputDir, Settings.FILE_NAME_TYPE_COUNTS), typeCounts);
		
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_LITERAL_COUNT), distinctLiterals.size());
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_TRIPLE_COUNT), tripleCount);
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_SUBJECT_COUNT), outdegreeCounts.size());
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_OBJECT_COUNT), indegreeCounts.size());
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_URI_COUNT), distinctUris.size());
		
		
		
		/**
		 * Write degree info
		 */
		DescriptiveStatistics stats = new DescriptiveStatistics();
		//outdegree (media/median/mode/range)
		
		for (PatriciaNode pNode: outdegreeCounts.elementSet()) stats.addValue(outdegreeCounts.count(pNode));
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_SUBJECT_COUNT), stats.getSum());
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_OUTDEGREE_AVG), stats.getMean());
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_OUTDEGREE_MEDIAN), stats.getPercentile(50));
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_OUTDEGREE_STD), stats.getStandardDeviation());
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_OUTDEGREE_RANGE), stats.getMax() - stats.getMin());
		//indegree (media/median/mode/range)
		stats.clear();
		for (PatriciaNode pNode: indegreeCounts.elementSet()) stats.addValue(indegreeCounts.count(pNode));
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_OBJECT_COUNT), stats.getSum());
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_INDEGREE_AVG), stats.getMean());
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_INDEGREE_MEDIAN), stats.getPercentile(50));
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_INDEGREE_STD), stats.getStandardDeviation());
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_INDEGREE_RANGE), stats.getMax() - stats.getMin());
		//degree (media/median/mode/range)
		HashMultiset<PatriciaNode> degrees = HashMultiset.create();
		degrees.addAll(indegreeCounts);
		degrees.addAll(outdegreeCounts);
		stats.clear();
		for (PatriciaNode pNode: outdegreeCounts.elementSet()) stats.addValue(degrees.count(pNode));
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_DEGREE_AVG), stats.getMean());
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_DEGREE_MEDIAN), stats.getPercentile(50));
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_DEGREE_STD), stats.getStandardDeviation());
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_DEGREE_RANGE), stats.getMax() - stats.getMin());
		
		
		
		
		//this one is a bit different (key is a set of strings)
		File nsTripleCountsFile = new File(datasetOutputDir, Settings.FILE_NAME_NS_TRIPLE_COUNTS);
		FileWriter namespaceTripleCountsOutput = new FileWriter(nsTripleCountsFile);
		for (com.google.common.collect.Multiset.Entry<Set<PatriciaNode>> entry: tripleNsCounts.entrySet()) {
			for (PatriciaNode pNode: entry.getElement()) {
				namespaceTripleCountsOutput.write(vault.redeem(pNode) + " ");
			}
			namespaceTripleCountsOutput.write("\t" + entry.getCount() + System.getProperty("line.separator"));
		}
		namespaceTripleCountsOutput.close();
		FileUtils.copyFile(Aggregator.PROVENANCE_FILE, new File(nsTripleCountsFile.getAbsolutePath() + ".sysinfo"));
		
		/**
		 * Finally, store the delta of this run
		 */
		FileUtils.write(new File(datasetOutputDir, Aggregator.DELTA_FILENAME), Integer.toString(Aggregator.DELTA_ID));
	}



	/**
	 * get nodes. if it is a uri, remove the < and >. For literals, keep quotes. This makes the number of substring operation later on low, and we can still distinguish between URIs and literals
	 * @param line
	 * @return
	 */
	public static String[] getNodes(String line) throws IndexOutOfBoundsException {
		int offset = 1;
		String sub = line.substring(offset, line.indexOf("> "));
		offset += sub.length()+3;
		String pred = line.substring(offset, line.indexOf("> ", offset));
		offset += pred.length() + 2;

		int endOffset = 2; //remove final ' .';
		if (line.charAt(offset) == '<') {
			//remove angular brackets
			offset++;
			endOffset++;
		}

		String obj = line.substring(offset, line.length() - endOffset);
//		return new String[]{sub.intern(), pred.intern(), obj.intern()};//this avoid gc mem cleanup errors, but comes with higher cpu cost as well
		return new String[]{sub, pred, obj};//only do intern on pred. compromise between avoiding gc mem cleanup errors, and cpu costs
	}

	private void processLine(String line) {
		String[] nodes;
		try {
			nodes = getNodes(line);
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
			indegreeCounts.add(sub.ticket);
//			distinctSubjects.add(sub.ticket);
//			distinctObjects.add(obj.ticket);
			PredicateCounter predCounter = null;
			if (!predicateCounts.containsKey(pred.ticket)) {
				predCounter = new PredicateCounter();
				predicateCounts.put(pred.ticket, predCounter);
			} else {
				predCounter = predicateCounts.get(pred.ticket);
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
			if (sub.isBnode) uniqBnodeCounts.add(sub.ticket);
			if (pred.isBnode) uniqBnodeCounts.add(pred.ticket);
			if (obj.isBnode) uniqBnodeCounts.add(obj.ticket);


			if (obj.isLiteral) {
				distinctLiterals.add(obj.ticket);
				if (obj.datatype != null) {
					dataTypeCounts.add(obj.datatype);
				}
				if (obj.langTag != null) {
					langTagCounts.add(obj.langTag);
				}
				if (obj.langTagWithoutReg!= null) {
					langTagWithoutRegCounts.add(obj.langTagWithoutReg);
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
				typeCounts.add(obj.ticket);
//			} else if (pred.isRdfs_domain || pred.isRdfs_range) {
//				propertyCounts.add(sub.stringRepresentation);
//				classCounts.add(obj.stringRepresentation);
//			} else if (pred.isRdfs_subClassOf) {
//				classCounts.add(sub.stringRepresentation);
//				classCounts.add(obj.stringRepresentation);
//			} else if (pred.isRdfs_subPropertyOf) {
//				propertyCounts.add(sub.stringRepresentation);
//				propertyCounts.add(obj.stringRepresentation);
			}
			
			
			
			
			//store URI info
			if (sub.isUri) distinctUris.add(sub.ticket);
			if (pred.isUri) distinctUris.add(pred.ticket);
			if (obj.isUri) distinctUris.add(obj.ticket);
		} else {
			System.out.println("Could not get triple from line. " + nodes.toString());
		}

	}
	/**
	 * just a simple helper method, to store the maps with a string as key, and counter as val
	 * @throws IOException 
	 */
	private void writeCountersToFile(File targetFile, HashMultiset<PatriciaNode> multiset) throws IOException {
		FileWriter fw = new FileWriter(targetFile);
		for (com.google.common.collect.Multiset.Entry<PatriciaNode> entry: multiset.entrySet()) {
			fw.write(vault.redeem((PatriciaNode)entry.getElement()) + "\t" + entry.getCount() + System.getProperty("line.separator"));
			
		}
		fw.close();
		//also store provenance
		FileUtils.copyFile(Aggregator.PROVENANCE_FILE, new File(targetFile.getAbsolutePath() + ".sysinfo"));
	}
	/**
	 * just a simple helper method, to store the maps with a string as key, and counter as val
	 * @throws IOException 
	 */
	private void writeStringCountersToFile(File targetFile, HashMultiset<String> multiset) throws IOException {
		FileWriter fw = new FileWriter(targetFile);
		for (com.google.common.collect.Multiset.Entry<String> entry: multiset.entrySet()) {
			fw.write(entry.getElement().toString() + "\t" + entry.getCount() + System.getProperty("line.separator"));
			
		}
		fw.close();
		//also store provenance
		FileUtils.copyFile(Aggregator.PROVENANCE_FILE, new File(targetFile.getAbsolutePath() + ".sysinfo"));
	}

	private void writePredCountersToFile(File targetDir, HashMap<PatriciaNode, PredicateCounter> predCounters) throws IOException {
		File predCountsFile = new File(targetDir, Settings.FILE_NAME_PREDICATE_COUNTS);
		FileWriter fwPredCounts = new FileWriter(predCountsFile);
		File predLitCountFiles = new File(targetDir, Settings.FILE_NAME_PREDICATE_LITERAL_COUNTS);
		FileWriter fwPredLitCounts = new FileWriter(predLitCountFiles);
		File predUriCountsFile = new File(targetDir, Settings.FILE_NAME_PREDICATE_NON_LIT_COUNTS);
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
		FileUtils.copyFile(Aggregator.PROVENANCE_FILE, new File(predCountsFile.getAbsolutePath() + ".sysinfo"));
		FileUtils.copyFile(Aggregator.PROVENANCE_FILE, new File(predLitCountFiles.getAbsolutePath() + ".sysinfo"));
		FileUtils.copyFile(Aggregator.PROVENANCE_FILE, new File(predUriCountsFile.getAbsolutePath() + ".sysinfo"));
	}
	private void writeSingleCountToFile(File targetFile, int val) throws IOException {
		FileUtils.writeStringToFile(targetFile, Integer.toString(val));
		FileUtils.copyFile(Aggregator.PROVENANCE_FILE, new File(targetFile.getAbsolutePath() + ".sysinfo"));
	}
	private void writeSingleCountToFile(File targetFile, double val) throws IOException {
		FileUtils.writeStringToFile(targetFile, Double.toString(val));
		FileUtils.copyFile(Aggregator.PROVENANCE_FILE, new File(targetFile.getAbsolutePath() + ".sysinfo"));
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
		File deltaFile = new File(datasetDir, Aggregator.DELTA_FILENAME);
		if (deltaFile.exists()) deltaFile.delete();
	}

	@Override
	public void run() {
		try {

			log("aggregating " + datasetDir.getName());
			delDelta();
			processDataset();
			Aggregator.PROCESSED_COUNT++;
			Aggregator.printProgress(datasetDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
