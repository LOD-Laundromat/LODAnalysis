package lodanalysis.utils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	
	
}
