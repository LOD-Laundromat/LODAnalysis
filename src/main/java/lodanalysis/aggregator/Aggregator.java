package lodanalysis.aggregator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import lodanalysis.Entry;
import lodanalysis.RuneableClass;
import lodanalysis.utils.Counter;
import lodanalysis.utils.NodeContainer;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

public class Aggregator  extends RuneableClass {
	Map<Set<String>, Counter> tripleNsCounts = new HashMap<Set<String>, Counter>();
	Map<String, Counter> dataTypeCounts = new HashMap<String, Counter>();
	Map<String, Counter> langTagCounts = new HashMap<String, Counter>();
	Map<String, Counter> langTagWithoutRegCounts = new HashMap<String, Counter>();
	Map<String, Counter> totalNsCounts = new HashMap<String, Counter>();
	Map<String, Counter> nsCountsUniq = new HashMap<String, Counter>();
	Set<String> uniqUris = new HashSet<String>();
	public Aggregator(Entry entry) throws IOException {
		super(entry);
		for (File datasetDir : entry.getDatasetDirs()) {
			processDataset(datasetDir);
		}
	}

	private void processDataset(File datasetDir) throws IOException {
		File inputFile = new File(datasetDir, "input.nt");
		if (inputFile.exists()) {
			FileInputStream is = new FileInputStream(inputFile);

			NxParser nxp = new NxParser(is);

			while (nxp.hasNext())
			     processLine(nxp.next());
			
			  
			  
			postProcessAnalysis();
			store(datasetDir);
		} else {
			System.out.println("no input file found in dataset " + datasetDir.getName());
		}
		
	}

	private void store(File datasetDir) throws IOException {
		String newLine = System.getProperty("line.separator");
		
		FileWriter nsCountsOutput = new FileWriter(new File(datasetDir, "namespaceCounts"));
		
		for (String ns: totalNsCounts.keySet()) {
			nsCountsOutput.write(ns + "\t" + totalNsCounts.get(ns) + newLine);
		}
		nsCountsOutput.close();
		
		FileWriter nsUniqCountsOutput = new FileWriter(new File(datasetDir, "namespaceUniqCounts"));
		for (String ns: nsCountsUniq.keySet()) {
			nsUniqCountsOutput.write(ns + "\t" + nsCountsUniq.get(ns) + newLine);
		}
		nsUniqCountsOutput.close();
		
		FileWriter namespaceTripleCountsOutput = new FileWriter(new File(datasetDir, "namespaceTripleCounts"));
		for (Set<String> tripleNs: tripleNsCounts.keySet()) {
			namespaceTripleCountsOutput.write(tripleNs.toString() + "\t" + tripleNsCounts.get(tripleNs) + newLine);
		}
		namespaceTripleCountsOutput.close();
		
		
		FileWriter languageTagCountsOutput = new FileWriter(new File(datasetDir, "languageTagCounts"));
		for (String langTag: langTagCounts.keySet()) {
			languageTagCountsOutput.write(langTag + "\t" + langTagCounts.get(langTag) + newLine);
		}
		languageTagCountsOutput.close();
		
		FileWriter langTagCountsWithoutRegionOutput = new FileWriter(new File(datasetDir, "langTagCountsWithoutRegion"));
		for (String langTag: langTagWithoutRegCounts.keySet()) {
			langTagCountsWithoutRegionOutput.write(langTag + "\t" + langTagWithoutRegCounts.get(langTag) + newLine);
		}
		langTagCountsWithoutRegionOutput.close();
		
		
		FileWriter dataTypeCountsOutput = new FileWriter(new File(datasetDir, "dataTypeCounts"));
		for (String langTag: dataTypeCounts.keySet()) {
			dataTypeCountsOutput.write(langTag + "\t" + dataTypeCounts.get(langTag) + newLine);
		}
		dataTypeCountsOutput.close();
	}

	private void postProcessAnalysis() {
		//we've got all the unique uris. Use these to count how diverse each namespace is used (i.e., the 'namespaceUniqCounts'
		for (String uri: uniqUris) {
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
			if (!totalNsCounts.containsKey(sub.ns)) {
				totalNsCounts.put(sub.ns, new Counter(1));
			} else {
				totalNsCounts.get(sub.ns).increase();
			}
			if (!totalNsCounts.containsKey(pred.ns)) {
				totalNsCounts.put(pred.ns, new Counter(1));
			} else {
				totalNsCounts.get(pred.ns).increase();
			}
			if (!obj.isLiteral) {
				if (!totalNsCounts.containsKey(obj.ns)) {
					totalNsCounts.put(obj.ns, new Counter(1));
				} else {
					totalNsCounts.get(obj.ns).increase();
				}
			}
			
			/**
			 * store uniq uris
			 */
			uniqUris.add(sub.stringRepresentation);
			uniqUris.add(pred.stringRepresentation);
			if (!obj.isLiteral) uniqUris.add(obj.stringRepresentation);
			
			if (obj.isLiteral) {
				/**
				 * store datatypes of literals
				 */
				if (!dataTypeCounts.containsKey(obj.datatype)) {
					dataTypeCounts.put(obj.datatype, new Counter(1));
				} else {
					dataTypeCounts.get(obj.datatype).increase();
				}
				
				/**
				 * store lang tag of literals
				 */
				if (!langTagCounts.containsKey(obj.langTag)) {
					langTagCounts.put(obj.langTag, new Counter(1));
				} else {
					langTagCounts.get(obj.langTag).increase();
				}
				if (!langTagWithoutRegCounts.containsKey(obj.langTagWithoutReg)) {
					langTagWithoutRegCounts.put(obj.langTagWithoutReg, new Counter(1));
				} else {
					langTagWithoutRegCounts.get(obj.langTagWithoutReg).increase();
				}
				
			}
			
			
			
			
		} else {
			System.out.println("Could not get triple from line. " + nodes.toString());
		}
		
	}

	private String getDataType(String obj) {
		// TODO Auto-generated method stub
		return null;
	}

	private void fetchNsTriples(String sub, String pred, String obj) {
		Set<String> tripleNs = new HashSet<String>();
//		if (subNs != null) tripleNs.add(subNs);
//		if (predNs != null) tripleNs.add(predNs);
//		if (objNs != null) tripleNs.add(objNs);
		
	}
	
	

}
