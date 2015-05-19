package lodanalysis.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import lodanalysis.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.data2semantics.vault.Vault;
import org.data2semantics.vault.PatriciaVault.PatriciaNode;

import com.google.common.collect.HashMultiset;

public class Utils {
	private static final String[] RELEVANT_SYS_PROPS = new String[]{
		"java.runtime.name",
		"java.vm.version",
		"java.vm.vendor",
		"java.runtime.version",
		"os.arch",
		"os.name",
		"sun.jnu.encoding",
		"java.specification.version",
		"java.vm.specification.version",
		"java.version"
	};
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
		File file = new File(datasetDir, Paths.INPUT_NT_GZ);
		if (!file.exists()) {
		    //try whether nquad exists
		    file = new File(datasetDir, Paths.INPUT_NQ_GZ);
		    if (!file.exists()) return false;//does not exist as well!
		}
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
	
	

	public static void printProgress(String msg, int totalCount, int processedCount) {
		String percentage = (String.format("%.0f%%",(100 * (float)processedCount) / (float) totalCount));
		System.out.print(msg + " (" + percentage + ")\r");
	}
	
	
	public static void writeSystemInfoToFile(File file) throws IOException {
		ArrayList<String> lines = getGitInfoLines();
		
		for (String key: RELEVANT_SYS_PROPS) {
			String prop = System.getProperty(key);
			lines.add(key + ": " + (prop != null? prop: "null"));
		}
		FileUtils.writeLines(file, lines);
	}
	public static ArrayList<String> getGitInfoLines() throws IOException
	{
		ArrayList<String> lines = new ArrayList<String>();
		Properties props = new Properties();
		props.load(Utils.class.getClassLoader().getResourceAsStream("git.properties"));
		lines.add(props.get("git.remote.origin.url").toString().replace("git@", "https://"));
		lines.add(props.get("git.branch").toString());
		lines.add(props.get("git.commit.id").toString());
		return lines;
	}
	
	
	public static int getDelta(File dir, String deltaFileName) throws IOException {
		int delta = -1;
		
		File deltaFile = new File(dir, deltaFileName);
		if (deltaFile.exists()) {
			delta = Integer.parseInt(FileUtils.readFileToString(deltaFile).trim());
		}
		return delta;
    }

    public static void writeSingleCountToFile(File targetFile, double val) throws IOException {
        FileUtils.writeStringToFile(targetFile, Double.toString(val));
    }

    public static void writeSingleCountToFile(File targetFile, int val) throws IOException {
        FileUtils.writeStringToFile(targetFile, Integer.toString(val));
    }

    /**
     * just a simple helper method, to store the maps with a string as key, and counter as val
     * @throws IOException 
     */
    public static void writePatriciaCountsToFile(Vault<String, PatriciaNode> vault, File targetFile, HashMultiset<PatriciaNode> multiset) throws IOException {
        FileWriter fw = new FileWriter(targetFile);
        for (com.google.common.collect.Multiset.Entry<PatriciaNode> entry: multiset.entrySet()) {
            fw.write(vault.redeem((PatriciaNode)entry.getElement()) + "\t" + entry.getCount() + System.getProperty("line.separator"));
            
        }
        fw.close();
        
    }
    public static int getLiteralLength(String string, int dataTypeLength, int langTagLength) {
            int literalLength = string.length() - 2; //subtract the two quotes
            
            
            if (dataTypeLength > 0) {
                //subtract datatype length, plus the two ^^
                literalLength -= dataTypeLength - 2;
            } else if (langTagLength > 0) {
                //also subtract the @
                literalLength -= langTagLength - 1;
            }
            return literalLength;
    }
    
    // based on jena, but heavily modified. Jena allows e.g. % or _ as ns
    // delimiter. I only want #, : and /
    public static String getNs(String uri) {
        char ch;
        int lg = uri.length();
        if (lg == 0)
            return "";
        int i = lg - 1;
        for (; i >= 1; i--) {
            ch = uri.charAt(i);
            if (ch == '#' || ch == ':' || ch == '/')
                break;
        }

        int j = i + 1;

        if (j >= lg)
            return uri;

        return uri.substring(0, j);
    }
    public static String getDataType(String literal) {
            if (literal.contains("^^")) {
                // probably a datatype
                int closingQuote = literal.lastIndexOf("\"");

                if (literal.length() <= closingQuote + 2 || literal.charAt(closingQuote + 1) != '^'
                        || literal.charAt(closingQuote + 2) != '^' || literal.charAt(closingQuote + 3) != '<') {
                    // ah, no lang tag after all!! either nothing comes after the
                    // quote, or something else than an '^^' follows
                } else {
                    return literal.substring(closingQuote + 4, literal.length() - 1);
//                    this.datatype = vault.store(stringRepresentation.substring(closingQuote + 4, stringRepresentation.length() - 1));
//                    this.dataTypeLength = stringRepresentation.length() - 1 - closingQuote - 4;
                }
            }
            return null;
    }
    
    public static String getLangTag(String literal) {
        String langTag = null;

        if (literal.indexOf('@') >= 0) {
            //this is probably a langtagged literal

            int closingQuote = literal.lastIndexOf("\"");
            if (literal.length() == closingQuote + 1 || literal.charAt(closingQuote+1) != '@') {
                //ah, no lang tag after all!! either nothing comes after the quote, or something else than an '@' follows
            } else {
                langTag = literal.substring(closingQuote + 2, literal.length());
            }
        }
        return langTag;
    }
    
}
