package lodanalysis.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import lodanalysis.Settings;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

public class Utils {

	public static Map<String, Integer> getCountsInDir(File dir) throws IOException {
		Map<String, Integer> counts = new HashMap<String, Integer>();

		for (File countFile : dir.listFiles()) {
			if (!countFile.isFile()) continue;
			LineIterator it = FileUtils.lineIterator(countFile, null);
			try {
				while (it.hasNext()) {
					String line = it.nextLine();
					if (line.length() > 0) {
						String[] cols = line.split("\\t");
						if (cols.length != 2) throw new IllegalStateException("Tried to get counts from line " + line + ", but coult not split by tab");
						counts.put(cols[0].intern(), Integer.parseInt(cols[1]));//Use intern!!! We share keys between hashmaps, so this memory optimization is really needed
					}
				}
			} finally {
				LineIterator.closeQuietly(it);
			}
		}
		if (counts.size() == 0) throw new IllegalStateException("No counts loaded from " + dir.getAbsolutePath());
		return counts;
	}
	public static Map<String, Integer> getCountsInFile(File countFile) throws IOException {
		Map<String, Integer> counts = new HashMap<String, Integer>();
		
		
		BufferedReader br = new BufferedReader(new FileReader(countFile), 120000);
		String line;
		while ((line = br.readLine()) != null) {
			if (line.length() > 0) {
				String[] cols = line.split("\\t");
				if (cols.length != 2) {
					br.close();
					throw new IllegalStateException("Tried to get counts from line " + line + ", but coult not split by tab");
				}
				counts.put(cols[0].intern(), Integer.parseInt(cols[1]));//Use intern!!! We share keys between hashmaps, so this memory optimization is really needed
			}
		}
		br.close();
		
		if (counts.size() == 0) throw new IllegalStateException("No counts loaded from " + countFile.getAbsolutePath());
		return counts;
	}
	
	
	/**
	 * file has input in form [http://bs, http://bd] 4
	 * @param countFile
	 * @return
	 * @throws IOException
	 */
	public static Map<Set<String>, Integer> getTripleCountsInFile(File countFile) throws IOException {
		Map<Set<String>, Integer> counts = new HashMap<Set<String>, Integer>();
		
		
		BufferedReader br = new BufferedReader(new FileReader(countFile), 120000);
		String line;
		while ((line = br.readLine()) != null) {
			if (line.length() > 0) {
				String[] cols = line.split("\\t");
				if (cols.length != 2) {
					br.close();
					throw new IllegalStateException("Tried to get counts from line " + line + ", but coult not split by tab");
				}
				int count = Integer.parseInt(cols[1]);
				String arrayString = cols[0].substring(1, cols[0].length()-1).trim();
				Set<String> namespaces = new HashSet<String>();
				if (arrayString.length() > 0) {
					namespaces = new HashSet<String>(Arrays.asList(arrayString.split(", ")));
				}
				counts.put(namespaces, count);
			}
		}
		br.close();
		
		if (counts.size() == 0) throw new IllegalStateException("No counts loaded from " + countFile.getAbsolutePath());
		return counts;
	}
	
	
	public static String getDatasetName(File datasetDir) throws IOException {
		String name = "";
		File basenameFile = new File(datasetDir, "basename");
		if (basenameFile.exists()) name = FileUtils.readFileToString(basenameFile).trim();
		return name;
	}
	
	public static boolean hasInputFileWithContent(File datasetDir) throws IOException{
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
}
