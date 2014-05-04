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
	
	//key: namespace, values: datasets and counts (both relative and absolute)
	private Map<String, Map<String, String>> namespaceCounts = new HashMap<String, Map<String, String>>();
	private Set<String> datasets = new HashSet<String>();
	private Map<String, String> authorities = new HashMap<String, String>();
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
		calcAuthorities();
		storeAuthorities();
		System.out.println();
	}
	
	public void printProgress(String msg, int totalCount, int processedCount)  {
		int intPercentage = Math.round((100 * (float)processedCount) / (float) totalCount);
		Integer storedPercentage = 0;
		if ((storedPercentage = progresses.get(msg)) != null && intPercentage == storedPercentage) return;
		progresses.put(msg, intPercentage);
		String percentage = Integer.toString(intPercentage) + "%";
		System.out.print(msg + " (" + percentage + ")\r");
	}

	private void storeAuthorities() throws IOException {
		//in other methods it was useful to have the namespace as key. now, as we are writing back to the dataset dirs, it is better to have the dataset as key.
		//So... convert!
		Map<String, Set<String>> authoritiesByDatasets = new HashMap<String, Set<String>>();
		for (String namespace: authorities.keySet()) {
			String dataset = authorities.get(namespace);
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
		
		Map<String, Integer> nsCountsForDataset = Utils.getCountsInFile(nsUniqCountsFile);
		int totalNsCount = 0;
		for (int count : nsCountsForDataset.values()) {
		    totalNsCount += count;
		}
		for (String namespace: nsCountsForDataset.keySet()) {
			int namespaceCount = nsCountsForDataset.get(namespace);
			Double relCount = (double)namespaceCount / (double)totalNsCount;
			
			if (!namespaceCounts.containsKey(namespace)) namespaceCounts.put(namespace, new HashMap<String, String>());
			namespaceCounts.get(namespace).put(datasetDir.getName(), getNsCountAsString(namespaceCount, relCount));
		}
	}
	
	private String getNsCountAsString(int absCount, double relCount) {
		return Integer.toString(absCount) + "-" + Double.toString(relCount);
	}
	
	private int getAbsCountFromString(String nsCount) {
		return Integer.parseInt(nsCount.split("-")[0]);
	}
	private double getRelCountFromString(String nsCount) {
		return Double.parseDouble(nsCount.substring(nsCount.indexOf('-')));
	}
	
	/**
	 * 
	 * @return mapping of key: namespaces, value: list of datasets, for which we could not determine the authority
	 * @throws IOException
	 */
	private Map<String, List<String>> calcAuthoritiesAbsolute() throws IOException {
		/**
		 * First, create map, where key = namespace, and value = datasets. The latter might be multiple when namespace counts is the same
		 */
		Map<String, List<String>> nsAuthoritiesWithDuplicates = new HashMap<String, List<String>>();
		int totalNsCounts = namespaceCounts.size();
		int calcCount = 0;
		for(String namespace: namespaceCounts.keySet()) {
			Map<String, String> datasetCounts = namespaceCounts.get(namespace);
			
			Integer largestVal = null;
			List<String> largestList = new ArrayList<String>();
			for (String dataset : datasetCounts.keySet()) {
				int absNsCount = getAbsCountFromString(datasetCounts.get(dataset));
			     if (largestVal == null || largestVal  < absNsCount){
			         largestVal = absNsCount;
			         largestList.clear();
			         largestList.add(dataset);
			     } else if (largestVal == absNsCount){
			         largestList.add(dataset);
			     }
			}
			nsAuthoritiesWithDuplicates.put(namespace, largestList);
			printProgress("creating authorities (first prune step, based on max ns declarations)", totalNsCounts, calcCount);
			calcCount++;
		}
		System.out.println();
		/**
		 * calculate authority based on abs ns occurance
		 */
		//key: namespace, val: dataset
		Set<String> namespaces = nsAuthoritiesWithDuplicates.keySet();
		totalNsCounts = namespaces.size();
		System.out.println("for " + totalNsCounts + " namespaces, selecting the authority using abs ns occurance");
		calcCount = 0;
		HashSet<String> namespacesToRemove = new HashSet<String>();
		for (String namespace: namespaces) {
			List<String> datasets = nsAuthoritiesWithDuplicates.get(namespace);
			if (datasets.size() == 1) {
				authorities.put(namespace, datasets.get(0));
				namespacesToRemove.add(namespace);
				
			}
			calcCount++;
			printProgress("selecting authorities based on abs ns occurance", totalNsCounts, calcCount);
		}
		System.out.println();
		System.out.println("managed to select " + namespacesToRemove.size() + " authorities based on abs ns occurance");
		for (String namespaceToRemove: namespacesToRemove) nsAuthoritiesWithDuplicates.remove(namespaceToRemove);
		return nsAuthoritiesWithDuplicates;
	}
	
	
	private Map<String, List<String>> calcAuthoritiesRelative(Map<String, List<String>> nsAuthoritiesWithDuplicates) throws IOException {
		/**
		 * First, create map, where key = namespace, and value = datasets. The latter might be multiple when namespace counts is the same
		 */
		Map<String, List<String>> nsAuthoritiesWithRelDuplicates = new HashMap<String, List<String>>();
		int totalNsCounts = nsAuthoritiesWithDuplicates.size();
		int calcCount = 0;
		for(String namespace: nsAuthoritiesWithDuplicates.keySet()) {
			List<String> datasets = nsAuthoritiesWithDuplicates.get(namespace);
			
			Double largestVal = null;
			List<String> largestList = new ArrayList<String>();
			for (String dataset : datasets) {
				double relNsCount = getRelCountFromString(namespaceCounts.get(namespace).get(dataset));
			     if (largestVal == null || largestVal  < relNsCount){
			         largestVal = relNsCount;
			         largestList.clear();
			         largestList.add(dataset);
			     }else if (largestVal == relNsCount){
			         largestList.add(dataset);
			     }
			}
			nsAuthoritiesWithRelDuplicates.put(namespace, largestList);
			printProgress("creating authorities (second prune step, based on relative max ns declarations)", totalNsCounts, calcCount);
			calcCount++;
		}
		System.out.println();
		/**
		 * calculate authority based on abs ns occurance
		 */
		//key: namespace, val: dataset
		Set<String> namespaces = nsAuthoritiesWithRelDuplicates.keySet();
		totalNsCounts = namespaces.size();
		System.out.println("for " + totalNsCounts + " namespaces, selecting the authority using relative ns occurance");
		calcCount = 0;
		HashSet<String> namespacesToRemove = new HashSet<String>();
		for (String namespace: namespaces) {
			List<String> datasets = nsAuthoritiesWithRelDuplicates.get(namespace);
			if (datasets.size() == 1) {
				authorities.put(namespace, datasets.get(0));
				namespacesToRemove.add(namespace);
			}
			calcCount++;
			printProgress("selecting authorities based on relative ns occurance", totalNsCounts, calcCount);
		}
		System.out.println();
		for (String namespaceToRemove: namespacesToRemove) nsAuthoritiesWithRelDuplicates.remove(namespaceToRemove);
		return nsAuthoritiesWithRelDuplicates;
	}

	private void selectRandomAuthorities(Map<String, List<String>> nsAuthoritiesWithDuplicates) throws IOException {
		Set<String> namespaces = nsAuthoritiesWithDuplicates.keySet();
		int totalNsCounts = namespaces.size();
		System.out.println("for " + totalNsCounts + " namespaces, selecting the authority at random");
		int calcCount = 0;
		for (String namespace: namespaces) {
			List<String> datasets = nsAuthoritiesWithDuplicates.get(namespace);
//			System.out.println("multiple datasets as authority for one namespace.... Picking at random");
//			
			int size = datasets.size();
			int item = new Random().nextInt(size); // In real life, the Random object should be rather more shared than this
			authorities.put(namespace, datasets.get(item));
//			int i = 0;
//			for(String datasetWithRelSize: datasetsWithRelSize)	{
//			    if (i == item) {
//			    	dataset = datasetWithRelSize;
//			    	String msg = "outcome: dataset " + dataset + " is authority for " + namespace;
//			    	System.out.println("outcome: dataset " + dataset + " is authority for " + namespace);
//			    	FileUtils.writeStringToFile(authorityLogFile, "duplicate auth. " + msg, "UTF-8", true);
//			    	break;
//			    }
//			    i++;
//			}
			
			calcCount++;
			printProgress("selecting authority at random", totalNsCounts, calcCount);
		}
		System.out.println();
//		
//		String dataset = null;
//		for (Double relSize: relativeNsSize.descendingKeySet()) {
//			//get firs val
//			Set<String> datasetsWithRelSize = relativeNsSize.get(relSize);
//			if (datasetsWithRelSize.size() > 1) {
//				System.out.println("multiple datasets as authority for one namespace.... Picking at random");
//				
//				int size = datasetsWithRelSize.size();
//				int item = new Random().nextInt(size); // In real life, the Random object should be rather more shared than this
//				int i = 0;
//				for(String datasetWithRelSize: datasetsWithRelSize)	{
//				    if (i == item) {
//				    	dataset = datasetWithRelSize;
//				    	String msg = "outcome: dataset " + dataset + " is authority for " + namespace;
//				    	System.out.println("outcome: dataset " + dataset + " is authority for " + namespace);
//				    	FileUtils.writeStringToFile(authorityLogFile, "duplicate auth. " + msg, "UTF-8", true);
//				    	break;
//				    }
//				    i++;
//				}
//			} else {
//				for(String datasetWithRelSize: datasetsWithRelSize)	{
//			    	dataset = datasetWithRelSize;
//			    	break;
//				}
//			}
//		}
	}
	
	/**
	 * 
	 * @return Map: key: namespace, value: dataset
	 * @throws IOException
	 */
	private void calcAuthorities() throws IOException {
		
		
		Map<String, List<String>> nsAuthoritiesWithDuplicates = calcAuthoritiesAbsolute();
		nsAuthoritiesWithDuplicates = calcAuthoritiesRelative(nsAuthoritiesWithDuplicates);
		selectRandomAuthorities(nsAuthoritiesWithDuplicates);
		
//		
//		/**
//		 * calculate authority: diffult
//		 * We have to load these dataset AGAIN, for every namespace... 
//		 * Load each dataset, one by one. Store the relative occurance of the NS in an object. Using this object, we can figure out the authority
//		 */
//		//we already remove the ones for which we could calc authority from the namespace object
//		namespaces = nsAuthoritiesWithDuplicates.keySet();
//		totalNsCounts = namespaces.size();
//		calcCount = 0;
//		for (String namespace: namespaces) {
//			List<String> datasets = nsAuthoritiesWithDuplicates.get(namespace);
//			nsAuthorities.put(namespace, selectBaseOnRelativeNumber(namespace, datasets));
//			calcCount++;
//			printProgress("selecting authorities (hard part, calculating relative occurance of ns in datasets)", totalNsCounts, calcCount);
//		}
		
	}

}
