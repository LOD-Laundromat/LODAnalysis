package lodanalysis.links;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import lodanalysis.Entry;
import lodanalysis.RuneableClass;
import lodanalysis.Settings;
import lodanalysis.utils.Utils;



public class CalcLinks extends RuneableClass {
	
	private Map<String, String> authorities = new HashMap<String, String>();//key: namespace, value: dataset
	BufferedWriter mainLatticeLinks;
	BufferedWriter simpleNsLinks;
	public CalcLinks(Entry entry) throws IOException {
		super(entry);
		String currentDir = new File("").getName();
		System.out.println("generating edge lists per dataset, as well as one big edge list in folder " +  currentDir);
		
		
		/**
		 * init writers
		 */
		mainLatticeLinks= new BufferedWriter(new FileWriter(new File(Settings.FILE_NAME_OUTLINK_LATTICE_NS)), 120768);
		simpleNsLinks= new BufferedWriter(new FileWriter(new File(Settings.FILE_NAME_OUTLINK_SIMPLE_NS)), 120768);
		authorities = Utils.getAuthorities(entry.getDatasetDirs());
		calcLinks();
		mainLatticeLinks.close();
		simpleNsLinks.close();
	}
	
	private void calcLinks() throws IOException {
		Set<File> datasetDirs = entry.getDatasetDirs();
		int totalCount = datasetDirs.size();
		int count = 0;
		
		for (File dataset: datasetDirs) {
			calcSimpleNsLink(dataset);
			calcLatticeLinks(dataset);
			Utils.printProgress("calculating links", totalCount, count);
			count++;
		}
		System.out.println();
	}
	
	/**
	 * per combination of namespaces in triples, we have the counts. (e.g. [:rdf, :foaf, :thisdataset] 10, means that there are 10 triples where the sub, pred and obj (not neccesarily in that order), belong to these three namespaces)
	 * Here, we use this to count the number of outgoing links, by only taking the triple where the current dataset is authority for.
	 * The example above would result in two links: ':thisdataset -> :rdf 10' and ':thisdataset -> :foaf 10'
	 * @param dataset
	 * @throws IOException
	 */
	private void calcLatticeLinks(File dataset) throws IOException {
		File nsCountFile = new File(dataset, Settings.FILE_NAME_NS_TRIPLE_COUNTS);
		if (!entry.strict() && !nsCountFile.exists()) return;//fail softly
		Map<Set<String>, Integer> nsCounts;
		try {
			nsCounts = Utils.getTripleCountsInFile(nsCountFile);
		} catch (IllegalStateException e) {
			if (!entry.strict()) return;//just ignore
			if (!Utils.hasInputFileWithContent(dataset)) {
				return;//no counts, because no input. nothing special
			} else {
				throw e;
			}
		}
		BufferedWriter out = new BufferedWriter(new FileWriter(new File(dataset, Settings.FILE_NAME_OUTLINK_LATTICE_NS)), 120768);
		Map<String, Integer> externalLinks = new HashMap<String, Integer>();
		for (Set<String> namespaces: nsCounts.keySet()) {
			
			Set<String> externalNamespaces = new HashSet<String>();
			boolean hasOwnNamespace = false;
			for (String namespace: namespaces) {
				String nsAuthority = authorities.get(namespace);
				if (nsAuthority == null) {
					out.close();
					throw new IllegalStateException("Hmmm, could not find authority for namespace " + namespaces + " , dataset " + dataset.getName());
				}
				if (nsAuthority.equals(dataset.getName())) {
					hasOwnNamespace = true;
				} else {
					externalNamespaces.add(namespace);
				}
			}
			if (hasOwnNamespace && externalNamespaces.size() > 0) {
				for (String externalNamespace: externalNamespaces) {
					Integer linkWeight = externalLinks.get(externalNamespace);
					if (linkWeight == null) {
						externalLinks.put(externalNamespace, 1);
					} else {
						externalLinks.put(externalNamespace, linkWeight + nsCounts.get(namespaces));
					}
				}
			}
		}
		for (String externalLink: externalLinks.keySet()) {
			out.write(externalLink + "\t" + authorities.get(externalLink) + "\t" + externalLinks.get(externalLink) + "\n");
			mainLatticeLinks.write(dataset.getName() + "\t" + authorities.get(externalLink) + "\t" + externalLinks.get(externalLink) + "\n");
		}
		out.close();
	}
	private void calcSimpleNsLink(File dataset) throws IOException {
		File nsCountFile = new File(dataset, Settings.FILE_NAME_NS_COUNTS);
		if (!entry.strict() && !nsCountFile.exists()) return;//fail softly
		Map<String, Integer> nsCounts;
		try {
			nsCounts = Utils.getCountsInFile(nsCountFile);
		} catch (IllegalStateException e) {
			if (!entry.strict()) return;//just ignore
			if (!Utils.hasInputFileWithContent(dataset)) {
				return;//no counts, because no input. nothing special
			} else {
				throw e;
			}
		}
		BufferedWriter out = new BufferedWriter(new FileWriter(new File(dataset, Settings.FILE_NAME_OUTLINK_SIMPLE_NS)), 120768);
		for (String namespace: nsCounts.keySet()) {
			if (namespace.equals("null")) continue;
			String nsAuthority = authorities.get(namespace);
			if (nsAuthority == null) {
				out.close();
				throw new IllegalStateException("Hmmm, could not find authority for namespace " + namespace);
			}
			if (nsAuthority.equals(dataset.getName())) continue; //points to itself.
			out.write(namespace + "\t" + nsAuthority + "\t" + nsCounts.get(namespace) + "\n");
			simpleNsLinks.write(dataset.getName() + "\t" + nsAuthority + "\t" + nsCounts.get(namespace) + "\n");
		}
		out.close();
	}
	
	
	
	
	
	

}
