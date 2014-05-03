package lodanalysis.links;

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



public class CalcLinks extends RuneableClass {
	
	private Map<String, String> authorities = new HashMap<String, String>();//key: namespace, value: dataset
	
	
	public CalcLinks(Entry entry) throws IOException {
		super(entry);
		
		getAuthorities();
		
		calcLinks();
	}
	
	
	private void calcLinks() throws IOException {
		Set<File> datasetDirs = entry.getDatasetDirs();
		int totalCount = datasetDirs.size();
		int count = 0;
		
		for (File dataset: datasetDirs) {
			calcSimpleNsLink(dataset);
			
			printProgress("calculating links", totalCount, count);
			count++;
		}
	}
	
	private void calcSimpleNsLink(File dataset) throws IOException {
		int totalNsCount = 0;
		Map<String, Integer> nsCounts = Utils.getCountsInFile(new File(dataset, Settings.FILE_NAME_NS_COUNTS));
		for (String namespace: nsCounts.keySet()) {
			int nsCount = nsCounts.get(namespace);
			totalNsCount++;
			String nsAuthority = authorities.get(namespace);
			if (nsAuthority == null) throw new IllegalStateException("Hmmm, could not find authority for namespace " + namespace);
		}
	}
	
	private void getAuthorities() throws IOException {
		
		Set<File> datasetDirs = entry.getDatasetDirs();
		int totalCount = datasetDirs.size();
		int count = 0;
		for (File dataset: datasetDirs) {
			BufferedReader br = new BufferedReader(new FileReader(new File(dataset, Settings.FILE_NAME_AUTHORITY)), 120000);
			String line;
			while ((line = br.readLine()) != null) {
				authorities.put(line, dataset.getName());
			}
			br.close();
			printProgress("retrieving authorities", totalCount, count);
			count++;
		}
	}
	
	
	
	private void printProgress(String msg, int totalCount, int processedCount) {
		String percentage = (String.format("%.0f%%",(100 * (float)processedCount) / (float) totalCount));
		System.out.print(msg + " (" + percentage + ")\r");
	}

}
