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
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import lodanalysis.Entry;
import lodanalysis.Settings;
import lodanalysis.utils.NodeContainer;

import org.apache.commons.io.FileUtils;

import com.google.common.collect.HashMultiset;

public class AggregateDataset implements Runnable  {
//	private static final String IGNORE_RDF_URI_PREFIX = "http://www.w3.org/1999/02/22-rdf-syntax-ns#_";
	private File datasetDir;
	private InputStream gzipStream;
	private InputStream fileStream;
	private Reader decoder;
	private BufferedReader reader;
	int literalCount = 0;
	int tripleCount = 0;
	HashMultiset<Set<String>> tripleNsCounts = HashMultiset.create();
	HashMultiset<String> dataTypeCounts = HashMultiset.create();
	HashMultiset<String> langTagCounts = HashMultiset.create();
	HashMultiset<String> langTagWithoutRegCounts = HashMultiset.create();
	HashMultiset<String> totalNsCounts = HashMultiset.create();
	HashMultiset<String> nsCountsUniq = HashMultiset.create();
	HashMultiset<String> uniqBnodeCounts = HashMultiset.create();
	HashMultiset<String> classCounts = HashMultiset.create();
	HashMultiset<String> predicateCounts = HashMultiset.create();
	Set<Integer> distinctSubjects = new HashSet<Integer>();
	Set<Integer> distinctObjects = new HashSet<Integer>();
	Set<Integer> distinctUris = new HashSet<Integer>();
	
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
		File datasetOutputDir = new File(entry.getOutputDir(), datasetMd5);
		if (!datasetOutputDir.exists()) datasetOutputDir.mkdir();
		writeCountersToFile(new File(datasetOutputDir, Settings.FILE_NAME_NS_COUNTS), totalNsCounts);
		writeCountersToFile(new File(datasetOutputDir, Settings.FILE_NAME_NS_UNIQ_COUNTS), nsCountsUniq);
		writeCountersToFile(new File(datasetOutputDir, Settings.FILE_NAME_LANG_TAG_COUNTS), langTagCounts);
		writeCountersToFile(new File(datasetOutputDir, Settings.FILE_NAME_LANG_TAG_NOREG_COUNTS), langTagWithoutRegCounts);
		writeCountersToFile(new File(datasetOutputDir, Settings.FILE_NAME_DATATYPE_COUNTS), dataTypeCounts);
		writeCountersToFile(new File(datasetOutputDir, Settings.FILE_NAME_UNIQ_BNODES_COUNTS), uniqBnodeCounts);
		writeCountersToFile(new File(datasetOutputDir, Settings.FILE_NAME_PREDICATE_COUNTS), predicateCounts);
		writeCountersToFile(new File(datasetOutputDir, Settings.FILE_NAME_CLASS_COUNTS), classCounts);
		
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_LITERAL_COUNT), literalCount);
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_TRIPLE_COUNT), tripleCount);
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_SUBJECT_COUNT), distinctSubjects.size());
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_OBJECT_COUNT), distinctObjects.size());
		writeSingleCountToFile(new File(datasetOutputDir, Settings.FILE_NAME_URI_COUNT), distinctUris.size());
		
		//this one is a bit different (key is a set of strings)
		File nsTripleCountsFile = new File(datasetOutputDir, Settings.FILE_NAME_NS_TRIPLE_COUNTS);
		FileWriter namespaceTripleCountsOutput = new FileWriter(nsTripleCountsFile);
		for (com.google.common.collect.Multiset.Entry<Set<String>> entry: tripleNsCounts.entrySet()) {
			namespaceTripleCountsOutput.write(entry.getElement().toString() + "\t" + entry.getCount() + System.getProperty("line.separator"));
		}
		namespaceTripleCountsOutput.close();
		FileUtils.copyFile(Aggregator.PROVENANCE_FILE, new File(nsTripleCountsFile.getAbsolutePath() + ".sysinfo"));
		
		/**
		 * Finally, store the delta of this run
		 */
		File deltaFile = new File(datasetOutputDir, Aggregator.DELTA_FILENAME);
		FileUtils.write(deltaFile, Integer.toString(Aggregator.DELTA_ID));
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
			NodeContainer sub = new NodeContainer(nodes[0], NodeContainer.Position.SUB);
			NodeContainer pred = new NodeContainer(nodes[1], NodeContainer.Position.PRED);
			NodeContainer obj = new NodeContainer(nodes[2], NodeContainer.Position.OBJ);
			
			
			/**
			 * Some generic counters
			 */
			tripleCount++;
			distinctSubjects.add(sub.stringRepresentation.hashCode());
			distinctObjects.add(obj.stringRepresentation.hashCode());
			
			/**
			 * store ns triples
			 */
			Set<String> tripleNs = new HashSet<String>();
			if (sub.ns != null) tripleNs.add(sub.ns);
			if (pred.ns != null) tripleNs.add(pred.ns);
			if (obj.ns != null) tripleNs.add(obj.ns);
			tripleNsCounts.add(tripleNs);

			/**
			 * store ns counters
			 */
			if (sub.isUri) totalNsCounts.add(sub.ns);
			if (pred.isUri) totalNsCounts.add(pred.ns);
			if (obj.isUri) totalNsCounts.add(obj.ns);

			
			/**
			 * store uniq namespaces
			 */
			if (sub.isUri) nsCountsUniq.add(sub.ns);
			if (pred.isUri) nsCountsUniq.add(pred.ns);
			if (obj.isUri) nsCountsUniq.add(obj.ns);

			/**
			 * store uniq bnodes
			 */
			if (sub.isBnode) uniqBnodeCounts.add(sub.stringRepresentation);
			if (pred.isBnode) uniqBnodeCounts.add(pred.stringRepresentation);
			if (obj.isBnode) uniqBnodeCounts.add(obj.stringRepresentation);


			if (obj.isLiteral) {
				if (obj.datatype != null) {
					dataTypeCounts.add(obj.datatype);
				}
				if (obj.langTag != null) {
					langTagCounts.add(obj.langTag);
				}
				if (obj.langTagWithoutReg!= null) {
					langTagWithoutRegCounts.add(obj.langTagWithoutReg);
				}
			}
			
			/**
			 * Store classes and props
			 */
			if (pred.isRdf_type) {
				classCounts.add(obj.stringRepresentation);
			} else if (pred.isRdfs_domain || pred.isRdfs_range) {
//				propertyCounts.add(sub.stringRepresentation);
				classCounts.add(obj.stringRepresentation);
			} else if (pred.isRdfs_subClassOf) {
				classCounts.add(sub.stringRepresentation);
				classCounts.add(obj.stringRepresentation);
//			} else if (pred.isRdfs_subPropertyOf) {
//				propertyCounts.add(sub.stringRepresentation);
//				propertyCounts.add(obj.stringRepresentation);
			}
			
			//store predicate info
			predicateCounts.add(pred.stringRepresentation);
			
			//storeLiteralInfo
			if (obj.isLiteral) literalCount++;
			
			//store URI info
			if (sub.isUri) distinctUris.add(sub.stringRepresentation.hashCode());
			if (pred.isUri) distinctUris.add(pred.stringRepresentation.hashCode());
			if (obj.isUri) distinctUris.add(obj.stringRepresentation.hashCode());
		} else {
			System.out.println("Could not get triple from line. " + nodes.toString());
		}

	}
	/**
	 * just a simple helper method, to store the maps with a string as key, and counter as val
	 * @throws IOException 
	 */
	private void writeCountersToFile(File targetFile, HashMultiset<String> multiset) throws IOException {
		FileWriter fw = new FileWriter(targetFile);
		for (com.google.common.collect.Multiset.Entry<String> entry: multiset.entrySet()) {
			fw.write(entry.getElement() + "\t" + entry.getCount() + System.getProperty("line.separator"));
		}
		fw.close();
		//also store provenance
		FileUtils.copyFile(Aggregator.PROVENANCE_FILE, new File(targetFile.getAbsolutePath() + ".sysinfo"));
	}
	private void writeSingleCountToFile(File targetFile, int val) throws IOException {
		FileUtils.writeStringToFile(targetFile, Integer.toString(val));
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
