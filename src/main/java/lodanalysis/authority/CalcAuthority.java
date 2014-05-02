package lodanalysis.authority;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;

import lodanalysis.Entry;
import lodanalysis.RuneableClass;
import lodanalysis.Settings;
import lodanalysis.utils.Utils;


/**
 * We calculate authority by:
 * - counting the number of unique namespaces per dataset
 * - And here, we:
 * 		- per namespace, get the dataset which uses this namespace the most (diversity-wise, as we use this unique list)
 * 		- the dataset which uses the most diverse set, is the authority.
 * 		- if two or more datasets draw, we analyze the relative use of this namespace by comparing to the use of other namespace for these datasets.
 * 		  The one with the largest relative use 'wins'
 */

public class CalcAuthority extends RuneableClass {
	
	
	//key: namespace, values: datasets and counts
	Map<String, Map<String, Integer>> namespaceCounts = new HashMap<String, Map<String, Integer>>();
	Set<String> datasets = new HashSet<String>();
	private File authorityLogFile;
	
	public CalcAuthority(Entry entry) throws IOException {
		super(entry);
		authorityLogFile = new File(entry.getDatasetParentDir(), Settings.FILE_NAME_LOG_AUTHORITY);
		if (authorityLogFile.exists()) authorityLogFile.delete();
		
		
		
		Set<File> datasetDirs = entry.getDatasetDirs();
		int totalDirCount = datasetDirs.size();
		int readCount = 0;
		for (File datasetDir : datasetDirs) {
			datasets.add(datasetDir.getName());//useful when writing back to dirs later
			retrieveNsCounts(datasetDir);
			readCount++;
			printProgress("reading ns counts to memory", totalDirCount, readCount);
		}
		System.out.println();
		storeAuthorities(calcAuthorities());
		System.out.println();
	}
	
	public void printProgress(String msg, int totalCount, int processedCount) throws IOException {
		String percentage = (String.format("%.0f%%",(100 * (float)processedCount) / (float) totalCount));
		System.out.print(msg + " (" + percentage + ")\r");
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
			FileUtils.writeLines(new File(entry.getDatasetParentDir(), dataset + "/" + Settings.FILE_NAME_AUTHORITY), authoritiesByDatasets.get(dataset));
		}
		//finally, initialize empty authority file for those datasets without authority (otherwise, we are not sure wheter a script has been run in a certain dataset dir when the authority file is missing)
		datasets.removeAll(authoritiesByDatasets.keySet());
		for (String dataset: datasets) {
			File authorityFile = new File(entry.getDatasetParentDir(), dataset + "/" + Settings.FILE_NAME_AUTHORITY);
			
			//if file already exists (perhaps previous analysis), delete
			if (authorityFile.exists()) authorityFile.delete();
			
			//write empty authority file
			authorityFile.createNewFile();
		}
		
	}
	
	private boolean hasInputFile(File datasetDir) {
		boolean hasFile = false;
		if (new File(datasetDir, Settings.FILE_NAME_INPUT_GZ).exists() || new File(datasetDir, Settings.FILE_NAME_INPUT).exists()) {
			hasFile = true;
		}
		return hasFile;
	}
	
	
	private void retrieveNsCounts(File datasetDir) throws IOException {
		
		File nsUniqCountsFile = new File(datasetDir, Settings.FILE_NAME_NS_UNIQ_COUNTS);
		if (!nsUniqCountsFile.exists()) {
			 if (hasInputFile(datasetDir)) {
				 throw new IllegalStateException("Could not find file containing ns counts: " + nsUniqCountsFile.getAbsolutePath());
			 } else {
				 //just ignore. we don't have counts, but we were not able to calc these counts as well. 
				 return;
			 }
		}
		
		for (java.util.Map.Entry<String, Integer> entry: Utils.getCountsInFile(nsUniqCountsFile).entrySet()) {
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
		
		Set<String> namespaces = nsAuthoritiesWithDuplicates.keySet();
		int totalNsCounts = namespaces.size();
		int calcCount = 0;
			
		Map<String, String> nsAuthorities = new HashMap<String, String>();
		for (String namespace: namespaces) {
			List<String> datasets = nsAuthoritiesWithDuplicates.get(namespace);
			if (datasets.size() == 1) {
				nsAuthorities.put(namespace, datasets.get(0));
			} else {
				//blegh, we need to properly analyze both datasets and hope we can find an autority
				nsAuthorities.put(namespace, selectBaseOnRelativeNumber(namespace, datasets));
			}
			calcCount++;
			printProgress("calculated authorities", totalNsCounts, calcCount);
		}
		return nsAuthorities;
	}
	
	/**
	 * Calculate the total number of namespaces. The dataset where the relative size of this particular namespace is the largest, 'wins' (i.e. is the authority)
	 */
	private String selectBaseOnRelativeNumber(String namespace, List<String> datasets) throws IOException {
		
		TreeMap<Double, Set<String>> relativeNsSize = new TreeMap<Double, Set<String>>();
		for (String dataset: datasets) {
			File nsUniqCountFile = new File(entry.getDatasetParentDir(), dataset + "/" + Settings.FILE_NAME_NS_UNIQ_COUNTS);
			if (!nsUniqCountFile.exists()) throw new IllegalStateException("Could not find dir containing ns counts: " + nsUniqCountFile.getAbsolutePath());
			
			Integer totalNsCount = 0;
			Integer currentNsCount = null;
			for (java.util.Map.Entry<String, Integer> entry: Utils.getCountsInFile(nsUniqCountFile).entrySet()) {
				totalNsCount += entry.getValue();
				if (entry.getKey().equals(namespace)) {
					currentNsCount = entry.getValue();
				}
			}
			if (currentNsCount == null) throw new IllegalStateException("Tried to find the namespace count for namespace " + namespace + ", in dataset " + dataset + ", but could not find it! It should be there though..");
			
			Double relsize = (double)currentNsCount / (double)totalNsCount;
			if (relativeNsSize.containsKey(relsize)) {
//				throw new IllegalStateException("We have a problem. We are ending up with two datasets who are both an authority of one namespace. "
//						+ "Hoped this wouldnt happen. Namespace: " + namespace + ". Datasets: " + dataset + " and " + relativeNsSize.get(relsize));
//				System.out.println("multiple authorities, one namespace....");
			}
			
			/**
			 * add this dataset, and the relative size of this ns, to our object
			 */
			Double relSize = (double)currentNsCount / (double)totalNsCount;
			Set<String> datasetsWithRelSize = relativeNsSize.get(relsize);
			if (datasetsWithRelSize != null) {
				datasetsWithRelSize.add(dataset);
			} else {
				datasetsWithRelSize = new HashSet<String>();
				datasetsWithRelSize.add(dataset);
				relativeNsSize.put(relSize, datasetsWithRelSize);
			}
		}
		
		
		
		String dataset = null;
		for (Double relSize: relativeNsSize.descendingKeySet()) {
			//get firs val
			Set<String> datasetsWithRelSize = relativeNsSize.get(relSize);
			if (datasetsWithRelSize.size() > 1) {
				System.out.println("multiple datasets as authority for one namespace.... Picking at random");
				
				int size = datasetsWithRelSize.size();
				int item = new Random().nextInt(size); // In real life, the Random object should be rather more shared than this
				int i = 0;
				for(String datasetWithRelSize: datasetsWithRelSize)	{
				    if (i == item) {
				    	dataset = datasetWithRelSize;
				    	String msg = "outcome: dataset " + dataset + " is authority for " + namespace;
				    	System.out.println("outcome: dataset " + dataset + " is authority for " + namespace);
				    	FileUtils.writeStringToFile(authorityLogFile, "duplicate auth. " + msg, "UTF-8", true);
				    	break;
				    }
				    i++;
				}
			} else {
				for(String datasetWithRelSize: datasetsWithRelSize)	{
			    	dataset = datasetWithRelSize;
			    	break;
				}
			}
		}
		return dataset;
	}
}
