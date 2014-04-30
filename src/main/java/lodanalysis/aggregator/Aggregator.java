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
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import lodanalysis.Entry;
import lodanalysis.RuneableClass;
import lodanalysis.utils.Counter;
import lodanalysis.utils.NodeContainer;

import org.apache.commons.io.FileUtils;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

public class Aggregator  extends RuneableClass {
	private static int DELTA_ID = 1;//useful when we re-run code. We store this id in each directory. When we re-run a (possibly newer) dataset dir, we can check whether we should re-analyze this dir, or skip it
	private static String DELTA_FILENAME = "aggregator_delta";
	private InputStream gzipStream;
	private InputStream fileStream;
	private Reader decoder;
	private BufferedReader reader;
	Map<Set<String>, Counter> tripleNsCounts = new HashMap<Set<String>, Counter>();
	Map<String, Counter> dataTypeCounts = new HashMap<String, Counter>();
	Map<String, Counter> langTagCounts = new HashMap<String, Counter>();
	Map<String, Counter> langTagWithoutRegCounts = new HashMap<String, Counter>();
	Map<String, Counter> totalNsCounts = new HashMap<String, Counter>();
	Map<String, Counter> nsCountsUniq = new HashMap<String, Counter>();
	Map<String, Counter> uniqUriCounts = new HashMap<String, Counter>();
	Map<String, Counter> uniqBnodeCounts = new HashMap<String, Counter>();
	
	public Aggregator(Entry entry) throws IOException {
		super(entry);

		File[] datasetDirs = entry.getDatasetDirs();
		int totalDirCount = datasetDirs.length;
		for (int i = 0; i < totalDirCount; i++) {
			File datasetDir = datasetDirs[i];
			String percentage = (String.format("%.0f%%",(100 * (float)i) / (float) totalDirCount));
			System.out.print("aggregating (" + percentage + ") " + getDatasetName(datasetDir) + "\r");
			if (getDelta(datasetDir) < DELTA_ID) {
				processDataset(datasetDir);
				storeDelta(datasetDir);
			} else {
				if (entry.isVerbose()) System.out.println("Skipping " + datasetDir.getName() + ". Already analyzed");
			}
			
		}
		System.out.println();
	}
	
	private void storeDelta(File datasetDir) throws IOException {
		File deltaFile = new File(datasetDir, DELTA_FILENAME);
		FileUtils.write(deltaFile, Integer.toString(DELTA_ID));
	}
	private int getDelta(File datasetDir) throws IOException {
		int delta = -1;
		File deltaFile = new File(datasetDir, DELTA_FILENAME);
		if (deltaFile.exists()) {
			delta = Integer.parseInt(FileUtils.readFileToString(deltaFile).trim());
		}
		return delta;
	}
	 
	private String getDatasetName(File datasetDir) throws IOException {
		String name = "";
		File basenameFile = new File(datasetDir, "basename");
		if (basenameFile.exists()) name = FileUtils.readFileToString(basenameFile).trim();
		return name;
	}

	private void processDataset(File datasetDir) throws IOException {
		try {
			File inputFile = new File(datasetDir, "input.nt.gz");
			if (!inputFile.exists()) inputFile = new File(datasetDir, "input.nt");
			if (inputFile.exists()) {
				BufferedReader br = getNtripleInputStream(inputFile);
	
				NxParser nxp = new NxParser(br);
	
				while (nxp.hasNext())
				     processLine(nxp.next());
				  
				postProcessAnalysis();
				store(datasetDir);
				close();
			} else {
				System.out.println("no input file found in dataset " + datasetDir.getName());
			}
		
		} catch (Throwable e) {
			//cancel on ALL exception. I want to know whats going on!
			System.out.println("Exception analyzing " + datasetDir.getName());
			e.printStackTrace();
		}
	}

	private void store(File datasetDir) throws IOException {
		writeCountersToFile(new File(datasetDir, "namespaceCounts"), totalNsCounts);
		writeCountersToFile(new File(datasetDir, "namespaceUniqCounts"), nsCountsUniq);
		writeCountersToFile(new File(datasetDir, "languageTagCounts"), langTagCounts);
		writeCountersToFile(new File(datasetDir, "langTagCountsWithoutRegion"), langTagWithoutRegCounts);
		writeCountersToFile(new File(datasetDir, "dataTypeCounts"), dataTypeCounts);
		writeCountersToFile(new File(datasetDir, "urisUniq"), uniqUriCounts);
		writeCountersToFile(new File(datasetDir, "bnodesUniq"), uniqBnodeCounts);
		
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
		}
	}

	private void processLine(Node[] nodes) {
		if (nodes.length == 3) {
			NodeContainer sub = new NodeContainer(nodes[0].toN3(), NodeContainer.Position.SUB);
			NodeContainer pred = new NodeContainer(nodes[1].toN3(), NodeContainer.Position.PRED);
			NodeContainer obj = new NodeContainer(nodes[2].toN3(), NodeContainer.Position.OBJ);
			
			
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
			if (pred.isUri) upCounter(totalNsCounts, pred.ns);
			if (obj.isUri) upCounter(totalNsCounts, obj.ns);
			
			/**
			 * store uniq uris
			 */
			if (sub.isUri) upCounter(uniqUriCounts, sub.stringRepresentation);
			if (pred.isUri) upCounter(uniqUriCounts, pred.stringRepresentation);
			if (obj.isUri) upCounter(uniqUriCounts, obj.stringRepresentation);
			
			/**
			 * store uniq bnodes
			 */
			if (sub.isBnode) upCounter(uniqBnodeCounts, sub.stringRepresentation);
			if (pred.isBnode) upCounter(uniqBnodeCounts, pred.stringRepresentation);
			if (obj.isBnode) upCounter(uniqBnodeCounts, obj.stringRepresentation);
			
			
			if (obj.isLiteral) {
				upCounter(dataTypeCounts, obj.datatype);
				upCounter(langTagCounts, obj.langTag);
				upCounter(langTagWithoutRegCounts, obj.langTagWithoutReg);
			}
		} else {
			System.out.println("Could not get triple from line. " + nodes.toString());
		}
		
	}
	
	/**
	 * just a simple helper method, to update the maps with a string as key, and counter as val
	 */
	private void upCounter(Map<String, Counter> map, String key) {
		if (!map.containsKey(key)) {
			map.put(key, new Counter(1));
		} else {
			map.get(key).increase();
		}
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
	
	private BufferedReader getNtripleInputStream(File file) throws IOException {
		reader = null;
		if (file.getName().endsWith(".gz")) {
			fileStream = new FileInputStream(file);
			gzipStream = new GZIPInputStream(fileStream);
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
}
