package lodanalysis.authority;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

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
	private Map<String, Integer> progresses = new HashMap<String, Integer>();
	
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
		System.out.println("" + namespaceCounts.size() + " namespaces to calc authority for");
		
		storeAuthorities(calcAuthorities());
		System.out.println();
	}
	
	public void printProgress(String msg, int totalCount, int processedCount) throws IOException {
		int intPercentage = Math.round((100 * (float)processedCount) / (float) totalCount);
		Integer storedPercentage = 0;
		if ((storedPercentage = progresses.get(msg)) != null && intPercentage == storedPercentage) return;
		progresses.put(msg, intPercentage);
		String percentage = Integer.toString(intPercentage) + "%";
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
//			System.out.println(dataset + authoritiesByDatasets.get(dataset));
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
		File file = new File(datasetDir, Settings.FILE_NAME_INPUT_GZ);
		if (file.exists()) {
			//check content
		} else {
			file = new File(datasetDir, Settings.FILE_NAME_INPUT);
			if (file.exists() && file.length() > 0) hasFile = true;
		}
		if (!file.exists()) new File(datasetDir, Settings.FILE_NAME_INPUT);
		if (file.exists() && file.length() > 0) {
			hasFile = true;
		}
		return hasFile;
	}
	
	private boolean hasInputFileWithContent(File datasetDir) throws IOException{
		File file = new File(datasetDir, Settings.FILE_NAME_INPUT_GZ);
		if (!file.exists()) file = new File(datasetDir, Settings.FILE_NAME_INPUT);
		BufferedReader reader = null;
		GZIPInputStream gzipStream = null;
		FileInputStream fileStream = null;
		if (file.getName().endsWith(".gz")) {
			fileStream = new FileInputStream(file);
			gzipStream = new GZIPInputStream(fileStream);//maximize buffer: http://java-performance.com/
			InputStreamReader decoder = new InputStreamReader(gzipStream, "UTF-8");
			reader = new BufferedReader(decoder);
		} else {
			reader = new BufferedReader(new FileReader(file));
		}
		
		boolean hasContent = reader.readLine() != null;
		
		reader.close();
		if (gzipStream != null) gzipStream.close();
		if (fileStream != null) fileStream.close();
		return hasContent;
	}
	
	
	private void retrieveNsCounts(File datasetDir) throws IOException {
		
		
		if (!hasInputFile(datasetDir)) return;
		File nsUniqCountsFile = new File(datasetDir, Settings.FILE_NAME_NS_UNIQ_COUNTS);
		if (!nsUniqCountsFile.exists() || nsUniqCountsFile.length() == 0) {
			if (!hasInputFileWithContent(datasetDir)) return;
//			throw new IllegalStateException("Could not find file containing ns counts: " + nsUniqCountsFile.getAbsolutePath());
			return;
			/**
			 * 
			 * 
			 * TODO: enable exception again! (disabled for debugging purposes, as not all files are aggregated)
			 * 
			 * 
			 */
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
		/**
		 * First, create map, where key = namespace, and value = datasets. The latter might be multiple when namespace counts is the same
		 */
		Map<String, List<String>> nsAuthoritiesWithDuplicates = new HashMap<String, List<String>>();
		int totalNsCounts = namespaceCounts.size();
		int calcCount = 0;
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
			printProgress("creating authorities (first prune step, based on max ns declarations)", totalNsCounts, calcCount);
			calcCount++;
		}
		//No need for namespacecounts anymore. Clean this up
		namespaceCounts = null;
		
		//key: namespace, val: dataset
		
		Set<String> namespaces = nsAuthoritiesWithDuplicates.keySet();
		totalNsCounts = namespaces.size();
		calcCount = 0;
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
			printProgress("calculating authorities (either by selecting the only dataset, or by selection based on rel. number)", totalNsCounts, calcCount);
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
