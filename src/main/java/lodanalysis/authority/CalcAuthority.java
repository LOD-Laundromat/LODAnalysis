package lodanalysis.authority;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import lodanalysis.Entry;
import lodanalysis.RuneableClass;
import lodanalysis.Utils;

public class CalcAuthority extends RuneableClass {
	Map<String, Map<String, Integer>> namespaceCounts = new HashMap<String, Map<String, Integer>>();
	
	public CalcAuthority(Entry entry) throws IOException {
		super(entry);
		for (File datasetDir : entry.getDatasetDirs()) {
			retrieveNsCounts(datasetDir);
		}
		calcAuthorities();
	}

	private void retrieveNsCounts(File datasetDir) throws IOException {
		
		File nsUniqCountDir = new File(datasetDir, "namespaceUniqCounts.nt");
		if (!nsUniqCountDir.exists())
			throw new IllegalStateException("Could not find dir containing ns counts");
		
		for (java.util.Map.Entry<String, Integer> entry: Utils.getCountsInDir(nsUniqCountDir).entrySet()) {
			String namespace = entry.getKey();
			if (!namespaceCounts.containsKey(namespace)) namespaceCounts.put(namespace, new HashMap<String, Integer>());
			namespaceCounts.get(namespace).put(datasetDir.getName(), entry.getValue());
		}
	}
	
	private void calcAuthorities() {
		//CAN A DATASET -NOT- have an authority???
		Map<String, String> authorities = new HashMap<String, String>();
		
		for(String namespace: namespaceCounts.keySet()) {
			Map<String, Integer> datasetCounts = namespaceCounts.get(namespace);
			
			
			
		}
	}
}
