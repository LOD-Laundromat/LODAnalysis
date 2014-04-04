package lodanalysis.authority;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;

import lodanalysis.Entry;
import lodanalysis.RuneableClass;
import lodanalysis.Utils;


/**
 * We calculate authority by:
 * - counting the number of unique namespaces per dataset (done in pig)
 * - And here, we:
 * 		- per namespace, get the dataset which uses this namespace the most (diversity-wise, as we use this unique list)
 * 		- the dataset which uses the most diverse set, is the authority.
 * 		- if two or more datasets draw, we analyze the relative use of this namespace by comparing to the use of other namespace for these datasets.
 * 		  The one with the largest relative use 'wins'
 */

public class CalcAuthority extends RuneableClass {
	private static String NS_UNIQUE_FILENAME = "namespaceUniqCounts";
	private static String AUTHORITY_FILENAME = "authority";
	
	//key: namespace, values: datasets and counts
	Map<String, Map<String, Integer>> namespaceCounts = new HashMap<String, Map<String, Integer>>();
	Set<String> datasets = new HashSet<String>();
	public CalcAuthority(Entry entry) throws IOException {
		super(entry);
		for (File datasetDir : entry.getDatasetDirs()) {
			datasets.add(datasetDir.getName());//useful when writing back to dirs later
			retrieveNsCounts(datasetDir);
		}
//		calcAuthorities();
		storeAuthorities(calcAuthorities());
	}

	private void storeAuthorities(Map<String, String> calcAuthorities) throws IOException {
		//in other methods it was useful to have the namespace as key. now, as we are writing back to the dataset dirs, it is better to have the dataset as key.
		//So... convert!
		Map<String, Set<String>> authoritiesByDatasets = new HashMap<String, Set<String>>();
		for (String namespace: calcAuthorities.keySet()) {
			String dataset = calcAuthorities.get(namespace);
			if (!authoritiesByDatasets.containsKey(dataset)) authoritiesByDatasets.put(dataset, new HashSet<String>());
			
			authoritiesByDatasets.get(dataset).add(namespace);
		}
		
		
		
		//now, for each dataset, write back
		for (String dataset: authoritiesByDatasets.keySet()) {
			System.out.println(dataset + authoritiesByDatasets.get(dataset));
			//overwrites, does not append
			FileUtils.writeLines(new File(entry.getDatasetParentDir(), dataset + "/" + AUTHORITY_FILENAME), authoritiesByDatasets.get(dataset));
		}
		//finally, initialize empty authority file for those datasets without authority (otherwise, we are not sure wheter a script has been run in a certain dataset dir when the authority file is missing)
		datasets.removeAll(authoritiesByDatasets.keySet());
		for (String dataset: datasets) {
			File authorityFile = new File(entry.getDatasetParentDir(), dataset + "/" + AUTHORITY_FILENAME);
			
			//if file already exists (perhaps previous analysis), delete
			if (authorityFile.exists()) authorityFile.delete();
			
			//write empty authority file
			authorityFile.createNewFile();
		}
		
	}

	private void retrieveNsCounts(File datasetDir) throws IOException {
		
		File nsUniqCountDir = new File(datasetDir, NS_UNIQUE_FILENAME);
		if (!nsUniqCountDir.exists())
			throw new IllegalStateException("Could not find dir containing ns counts: " + nsUniqCountDir.getAbsolutePath());
		
		for (java.util.Map.Entry<String, Integer> entry: Utils.getCountsInDir(nsUniqCountDir).entrySet()) {
			String namespace = entry.getKey();
			if (!namespaceCounts.containsKey(namespace)) namespaceCounts.put(namespace, new HashMap<String, Integer>());
			namespaceCounts.get(namespace).put(datasetDir.getName(), entry.getValue());
		}
	}
	
	/**
	 * 
	 * @return Map: key: namespace, value: dataset
	 * @throws IOException
	 */
	private Map<String, String> calcAuthorities() throws IOException {
		//key: namespace, value: datasets (might be multiple, when namespacecounts is the same)
		Map<String, List<String>> nsAuthoritiesWithDuplicates = new HashMap<String, List<String>>();
		
		for(String namespace: namespaceCounts.keySet()) {
			Map<String, Integer> datasetCounts = namespaceCounts.get(namespace);
			
			Integer largestVal = null;
			List<String> largestList = new ArrayList<String>();
			for (java.util.Map.Entry<String, Integer> i : datasetCounts.entrySet()){
			     if (largestVal == null || largestVal  < i.getValue()){
			         largestVal = i.getValue();
			         largestList.clear();
			         largestList .add(i.getKey());
			     }else if (largestVal == i.getValue()){
			         largestList.add(i.getKey());
			     }
			}
			nsAuthoritiesWithDuplicates.put(namespace, largestList);
		}
		//No need for namespacecounts anymore. Clean this up
		namespaceCounts = null;
		
		//key: namespace, val: dataset
		Map<String, String> nsAuthorities = new HashMap<String, String>();
		for (String namespace: nsAuthoritiesWithDuplicates.keySet()) {
			List<String> datasets = nsAuthoritiesWithDuplicates.get(namespace);
			if (datasets.size() == 1) {
				nsAuthorities.put(namespace, datasets.get(0));
			} else {
				nsAuthorities.put(namespace, selectBaseOnRelativeNumber(namespace, datasets));
			}
		}
		return nsAuthorities;
	}
	private String selectBaseOnRelativeNumber(String namespace, List<String> datasets) throws IOException {
		
		TreeMap<Double, String> relativeNsSize = new TreeMap<Double, String>();
		for (String dataset: datasets) {
			File nsUniqCountDir = new File(entry.getDatasetParentDir(), "dataset/" + NS_UNIQUE_FILENAME);
			if (!nsUniqCountDir.exists()) throw new IllegalStateException("Could not find dir containing ns counts: " + nsUniqCountDir.getAbsolutePath());
			
			Integer totalNsCount = 0;
			Integer currentNsCount = null;
			for (java.util.Map.Entry<String, Integer> entry: Utils.getCountsInDir(nsUniqCountDir).entrySet()) {
				totalNsCount += entry.getValue();
				if (entry.getKey().equals(namespace)) {
					currentNsCount = entry.getValue();
				}
			}
			if (currentNsCount == null) throw new IllegalStateException("Tried to find the namespace count for namespace " + namespace + ", in dataset " + dataset + ", but could not find it! It should be there though..");
			Double relsize = (double)currentNsCount / (double)totalNsCount;
			if (relativeNsSize.containsKey(relsize)) throw new IllegalStateException("We have a problem. We are ending up with two datasets who are both an authority of one namespace. "
					+ "Hoped this wouldnt happen. Namespace: " + namespace + ". Datasets: " + dataset + " and " + relativeNsSize.get(relsize));
			relativeNsSize.put((double)currentNsCount / (double)totalNsCount, dataset);
		}
		String dataset = null;
		for (Double relSize: relativeNsSize.descendingKeySet()) {
			//get firs val
			dataset = relativeNsSize.get(relSize);
			break;
		}
		return dataset;
	}
}
