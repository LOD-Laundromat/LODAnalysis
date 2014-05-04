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
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.List;


import lodanalysis.Entry;
import lodanalysis.Settings;
import lodanalysis.utils.Counter;
import lodanalysis.utils.NodeContainer;

import org.apache.commons.io.FileUtils;

public class AggregateDataset implements Runnable  {
	private final String RDFS_SUBCLASSOF = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
	private final String RDFS_SUBPROPERTYOF = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
	private final String RDFS_DOMAIN = "http://www.w3.org/2000/01/rdf-schema#domain";
	private final String RDFS_RANGE = "http://www.w3.org/2000/01/rdf-schema#range";
	private final String RDFS_DATATYPE = "http://www.w3.org/2000/01/rdf-schema#Datatype";
	private final String OWL_SAMEAS = "http://www.w3.org/2002/07/owl#sameAs";
	private final String OWL_EQUCLASS = "http://www.w3.org/2002/07/owl#equivalentClass";
	private final String OWL_EQUPROPERTY = "http://www.w3.org/2002/07/owl#equivalentProperty";
	private File datasetDir;
	private InputStream gzipStream;
	private InputStream fileStream;
	private Reader decoder;
	private BufferedReader reader;
	Set<String> classSet = new HashSet<String>();
	Set<String> propertySet = new HashSet<String>();
	Map<String, Set<String>> sameAsSubjectSet = new HashMap<String, Set<String>>();
	Map<String, Set<String>> sameAsObjectSet = new HashMap<String, Set<String>>();
	Map<Set<String>, Counter> tripleNsCounts = new HashMap<Set<String>, Counter>();
	Map<String, Counter> dataTypeCounts = new HashMap<String, Counter>();
	Map<String, Counter> langTagCounts = new HashMap<String, Counter>();
	Map<String, Counter> langTagWithoutRegCounts = new HashMap<String, Counter>();
	Map<String, Counter> totalNsCounts = new HashMap<String, Counter>();
	Map<String, Counter> nsCountsUniq = new HashMap<String, Counter>();
	Map<String, Counter> uniqUriCounts = new HashMap<String, Counter>();
	Map<String, Counter> uniqBnodeCounts = new HashMap<String, Counter>();
	Map<String, Counter> schemaCounts = new HashMap<String, Counter>();
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
			if (!inputFile.exists()) inputFile = new File(datasetDir, Settings.FILE_NAME_INPUT);
			if (inputFile.exists()) {
				BufferedReader br = getNtripleInputStream(inputFile);
				String line = null;
//				int count = 0;
				while((line = br.readLine())!= null) {
					processLine(line);
//					count++;
//					if (count % 1000000 == 0) System.out.println(count);
				}

				postProcessAnalysis();
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
		writeCountersToFile(new File(datasetDir, Settings.FILE_NAME_NS_COUNTS), totalNsCounts);
		writeCountersToFile(new File(datasetDir, Settings.FILE_NAME_NS_UNIQ_COUNTS), nsCountsUniq);
		writeCountersToFile(new File(datasetDir, Settings.FILE_NAME_LANG_TAG_COUNTS), langTagCounts);
		writeCountersToFile(new File(datasetDir, Settings.FILE_NAME_LANG_TAG_NOREG_COUNTS), langTagWithoutRegCounts);
		writeCountersToFile(new File(datasetDir, Settings.FILE_NAME_DATATYPE_COUNTS), dataTypeCounts);
		writeCountersToFile(new File(datasetDir, Settings.FILE_NAME_UNIQ_URIS_COUNTS), uniqUriCounts);
		writeCountersToFile(new File(datasetDir, Settings.FILE_NAME_UNIQ_BNODES_COUNTS), uniqBnodeCounts);
		writeCountersToFile(new File(datasetDir, Settings.FILE_NAME_SCHEMA_URI_COUNTS), schemaCounts);

		writeSetToFile(new File(datasetDir, Settings.FILE_NAME_CLASSES), classSet);
		writeSetToFile(new File(datasetDir, Settings.FILE_NAME_PROPERTIES), propertySet);

		//this one is a bit different (key is a set of strings)
		FileWriter namespaceTripleCountsOutput = new FileWriter(new File(datasetDir, "namespaceTripleCounts"));
		for (Set<String> tripleNs: tripleNsCounts.keySet()) {
			namespaceTripleCountsOutput.write(tripleNs.toString() + "\t" + tripleNsCounts.get(tripleNs) + System.getProperty("line.separator"));
		}
		namespaceTripleCountsOutput.close();
	}

	private void postProcessAnalysis() {
		//we've got all the unique uris. Use these to count how diverse each namespace is used (i.e., the 'namespaceUniqCounts'
		for (String uri: uniqUriCounts.keySet()) {
			String ns = NodeContainer.getNs(uri);
			if (!nsCountsUniq.containsKey(ns)) {
				nsCountsUniq.put(ns, new Counter(1));
			} else {
				nsCountsUniq.get(ns).increase();
			}
			processSameAsChain(classSet);
			processSameAsChain(propertySet);
		}
	}

	/**
	 * This function basically applies the sameAs transitivity until it finds all the
	 * transitivity relationships among the items in the given set; since it avoids
	 * recursion, its not that readable ;-)
	 */
	private void processSameAsChain(Set<String> set) {
		List<String> temp = new ArrayList<String>();

		do {
			for (String item : set) {
				/* ----------------------------------- */
				if (sameAsSubjectSet.get(item) != null) {
					for (String s : sameAsSubjectSet.get(item)) {
						if (!set.contains(s)){
							temp.add(s);
						}
					}
				}
				/* ----------------------------------- */
				if (sameAsObjectSet.get(item) != null) {
					for (String s : sameAsObjectSet.get(item)) {
						if (!set.contains(s)){
							temp.add(s);
						}
					}
				}
			}
			/* ----------------------------------- */
			if (temp.size() > 0) {
				for (String s : temp) {
					set.add(s);
				}
				temp.clear();
			} else {
				break;
			}
		} while (true);
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
		return new String[]{sub, pred.intern(), obj};//only do intern on pred. compromise between avoiding gc mem cleanup errors, and cpu costs
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
			 * Collecting and counting schema URIs
			 */
			if (sub.isSchema)
				upCounter(schemaCounts, sub.stringRepresentation);
			if (pred.isSchema) {
				upCounter(schemaCounts, pred.stringRepresentation);
				if (pred.stringRepresentation.equals(RDFS_SUBCLASSOF) ||
				    pred.stringRepresentation.equals(OWL_EQUCLASS)) {
					if (!isBlankNode (sub.stringRepresentation)) classSet.add(sub.stringRepresentation);
					if (!isBlankNode (obj.stringRepresentation)) classSet.add(obj.stringRepresentation);

				} else if (pred.stringRepresentation.equals(OWL_SAMEAS)) {
				} else if (pred.stringRepresentation.equals(RDFS_RANGE) ||
					   pred.stringRepresentation.equals (RDFS_DOMAIN)) {
					if (!isBlankNode (sub.stringRepresentation)) propertySet.add (sub.stringRepresentation);
					if (!isBlankNode (obj.stringRepresentation)) classSet.add (obj.stringRepresentation);
				} else if (pred.stringRepresentation.equals (RDFS_SUBPROPERTYOF) ||
					   pred.stringRepresentation.equals (OWL_EQUPROPERTY))	{
					if (!isBlankNode (sub.stringRepresentation)) propertySet.add (sub.stringRepresentation);
					if (!isBlankNode (obj.stringRepresentation)) classSet.add (obj.stringRepresentation);
				} else if (pred.stringRepresentation.equals (RDFS_DATATYPE)) {
					if (!isBlankNode (sub.stringRepresentation)) classSet.add (sub.stringRepresentation);
				}
			}
			if (obj.isSchema)
				upCounter(schemaCounts, obj.stringRepresentation);

			/**
			 * store ns triples
			 */
			Set<String> tripleNs = new HashSet<String>();
			if (sub.ns != null) tripleNs.add(sub.ns);
			if (pred.ns != null) tripleNs.add(pred.ns);
			if (obj.ns != null) tripleNs.add(obj.ns);
			if (!tripleNsCounts.containsKey(tripleNs)) {
				tripleNsCounts.put(tripleNs, new Counter(1));
			} else {
				tripleNsCounts.get(tripleNs).increase();
			}

			/**
			 * store ns counters
			 */
			if (sub.isUri) upCounter(totalNsCounts, sub.ns);
			if (pred.isUri) upCounter(totalNsCounts, pred.ns, true);
			if (obj.isUri) upCounter(totalNsCounts, obj.ns);

			/**
			 * store uniq uris
			 */
			if (sub.isUri) upCounter(uniqUriCounts, sub.stringRepresentation);
			if (pred.isUri) upCounter(uniqUriCounts, pred.stringRepresentation, true);
			if (obj.isUri) upCounter(uniqUriCounts, obj.stringRepresentation);

			/**
			 * store uniq bnodes
			 */
			if (sub.isBnode) upCounter(uniqBnodeCounts, sub.stringRepresentation);
			if (pred.isBnode) upCounter(uniqBnodeCounts, pred.stringRepresentation);
			if (obj.isBnode) upCounter(uniqBnodeCounts, obj.stringRepresentation);


			if (obj.isLiteral) {
				if (obj.datatype != null) {
					upCounter(dataTypeCounts, obj.datatype);
				}
				if (obj.langTag != null) {
					upCounter(langTagCounts, obj.langTag);
				}
				if (obj.langTagWithoutReg!= null) {
					upCounter(langTagWithoutRegCounts, obj.langTagWithoutReg);
				}
			}
		} else {
			System.out.println("Could not get triple from line. " + nodes.toString());
		}

	}
	private void upCounter(Map<String, Counter> map, String key) {
		upCounter(map, key, false);
	}
	/**
	 * just a simple helper method, to update the maps with a string as key, and counter as val
	 */
	private void upCounter(Map<String, Counter> map, String key, boolean internKey) {
		if (key == null) key = "null";
		Counter counter = map.get(key);
		if (counter == null) {
			counter = new Counter(1);
			if (internKey) {
				map.put(key.intern(), counter);
			} else {

				map.put(key, counter);
			}
		}
		counter.increase();
	}
	private boolean isBlankNode (String node) {
		return node.startsWith ("_:") ? true : false;
	}
	/**
	 * just a simple helper method, to store the maps with a string as key, and counter as val
	 * @throws IOException 
	 */
	private void writeCountersToFile(File targetFile, Map<String, Counter> map) throws IOException {
		FileWriter fw = new FileWriter(targetFile);
		for (String key: map.keySet()) {
			fw.write(key + "\t" + map.get(key) + System.getProperty("line.separator"));
		}
		fw.close();
	}
	/**
	 * just a simple helper method, to store the sets of strings to a file
	 * @throws IOException
	 */
	private void writeSetToFile(File targetFile, Set<String> set) throws IOException {
		FileWriter fw = new FileWriter(targetFile);
		for (String str: set) {
			fw.write(str + System.getProperty("line.separator"));
		}
		fw.close();
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

	private void storeDelta() throws IOException {
		//hmpf, when an exception occurs, strangely, we still write the delta, even though we havent written the other results. So check! (I know, a bit hacky, but reproducing this mem exception is annoying)
		File nsOutputFile = new File(datasetDir, Settings.FILE_NAME_NS_UNIQ_COUNTS);
		if (nsOutputFile.exists()) {
			File deltaFile = new File(datasetDir, Aggregator.DELTA_FILENAME);
			FileUtils.write(deltaFile, Integer.toString(Aggregator.DELTA_ID));
		}
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
			storeDelta();
			Aggregator.PROCESSED_COUNT++;
			Aggregator.printProgress(datasetDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
