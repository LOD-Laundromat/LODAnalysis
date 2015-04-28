package lodanalysis.streamer.singlerun;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.zip.GZIPInputStream;

import lodanalysis.Entry;
import lodanalysis.Paths;
import lodanalysis.streamer.NodeWrapper;

import org.data2semantics.vault.PatriciaVault;
import org.data2semantics.vault.PatriciaVault.PatriciaNode;
import org.data2semantics.vault.Vault;

import com.google.common.collect.HashMultiset;

public class StreamDatasetNamespaces implements Runnable  {
	public class PredicateCounter {
		int count = 1;//how often does this predicate occur. (initialize with 1)
		//how many literals (objects) does it co-occur with
		//how many URIs/bnodes (objects) does it co-occur with
		int objNonLiteralCount = 0;
		HashSet<PatriciaNode> distinctObjNonLiteralCount = new HashSet<PatriciaNode>();
        //how many distinct subjects (URIs or bnodes) does it co-occur with
        HashSet<PatriciaNode> distinctSubCount = new HashSet<PatriciaNode>();
	}

	private File datasetDir;
	private InputStream gzipStream;
	private InputStream fileStream;
	private Reader decoder;
	private BufferedReader reader;
	private Vault<String, PatriciaNode> vault =  new PatriciaVault();
	private boolean isNquadFile = false;
	int tripleCount = 0;
	private HashMultiset<PatriciaNode> nsCounts = HashMultiset.create();
	
	private Entry entry;
	public static void stream(Entry entry, File datasetDir) throws IOException {
		StreamDatasetNamespaces aggr = new StreamDatasetNamespaces(entry, datasetDir);

		aggr.run();
	}
	public StreamDatasetNamespaces(Entry entry, File datasetDir) throws IOException {
		this.entry = entry;
		this.datasetDir = datasetDir;
	}

	private void processDataset() throws IOException {
		try {
			File inputFile = new File(datasetDir, Paths.INPUT_NT_GZ);
			if (!inputFile.exists()) {
			    inputFile = new File(datasetDir, Paths.INPUT_NQ_GZ);
			    isNquadFile = true;
			}
			if (inputFile.exists()) {
				BufferedReader br = getNtripleInputStream(inputFile);
				String line = null;
				boolean somethingRead = false;
				while((line = br.readLine())!= null) {
					somethingRead = true;
					processLine(line);
				}
				if (somethingRead) {
					store();
				} else {
					log("empty input file found in dataset " + datasetDir.getName());
				}
				
				close();
			} else {
				log("no input file found in dataset " + datasetDir.getName());
			}

		} catch (Throwable e) {
			//cancel on ALL exception. I want to know whats going on!
			System.out.println("Exception analyzing " + datasetDir.getName());
			e.printStackTrace();
		}
	}

	private void log(String msg) throws IOException {
		if (entry.isVerbose()) System.out.println(msg);
	}

	private void store() throws IOException {
	    
		String datasetMd5 = datasetDir.getName();
		File datasetOutputDir = new File(entry.getMetricParentDir(), datasetMd5);
		if (!datasetOutputDir.exists()) datasetOutputDir.mkdir();
		
		writePatriciaCountsToFile(new File(datasetOutputDir, Paths.NS_COUNTS), nsCounts);
	
	}

	/**
     * just a simple helper method, to store the maps with a string as key, and counter as val
     * @throws IOException 
     */
    private void writePatriciaCountsToFile(File targetFile, HashMultiset<PatriciaNode> multiset) throws IOException {
        FileWriter fw = new FileWriter(targetFile);
        for (com.google.common.collect.Multiset.Entry<PatriciaNode> entry: multiset.entrySet()) {
            fw.write(vault.redeem((PatriciaNode)entry.getElement()) + "\t" + entry.getCount() + System.getProperty("line.separator"));
            
        }
        fw.close();
        
    }

	/**
	 * get nodes. if it is a uri, remove the < and >. For literals, keep quotes. This makes the number of substring operation later on low, and we can still distinguish between URIs and literals
	 * @param line
	 * @return
	 */
	public static String[] parseStatement(String line, boolean isNquadFile) throws IndexOutOfBoundsException {
		int offset = 1;//remove first <
		String sub = line.substring(offset, line.indexOf("> "));
		offset += sub.length()+3;//remove '> <'
		String pred = line.substring(offset, line.indexOf("> ", offset));
		offset += pred.length() + 2;//remove '> '
		
		
		
		int endIndex = line.lastIndexOf(' '); //remove final ' .';
		boolean objIsUri = false;
		if (line.charAt(offset) == '<') {
		    //a uri
		    objIsUri = true;
		    endIndex--;//remove '>'
			offset++;//remove '<' as well
		}
		String obj = line.substring(offset, endIndex);
		String graph = null;
		
		
		if (isNquadFile) {
		    //there might be a graph specified in this statement
		    if (objIsUri) {
		        int separatorIndex = obj.indexOf(' ');
		        if (separatorIndex > 0) {
		            //ah, this line has graph specified
		            graph = obj.substring(separatorIndex + 2);//remove ' <' of ng ('>' is already removed)
		            obj = obj.substring(0, separatorIndex-1);//remove '>' of obj
		        }
		    } else {
		        if (obj.charAt(obj.length() - 1) == '>' && obj.lastIndexOf(' ') > obj.lastIndexOf('^')) {
		            //ah, this line has graph specified. Tricky condition, because we don't want to confuse datatyped literals: "literal"^^<datatype>
		            int separatorIndex = obj.lastIndexOf(' ');
		            graph = obj.substring(separatorIndex + 2, obj.length() - 1);
		            obj = obj.substring(0, separatorIndex);
		        }
		    }
		    
		    
		}
	    return new String[]{sub, pred, obj, graph};
	}

	private void processLine(String line) {
		String[] nodes;
		try {
			nodes = parseStatement(line, isNquadFile);
		} catch (Exception e) {
			// Invalid triples. In our class it should never happen
			return;
		}
		if (nodes.length >= 3) {
		    tripleCount++;
            //get distinct uris
            if (nodes[0].length() > 0 && nodes[0].charAt(0) != '"') {
                if (!nodes[0].startsWith(NodeWrapper.BNODE_SUBSTRING)) nsCounts.add(vault.store(NodeWrapper.getNs(nodes[0])));
            }
            if (nodes[1].length() > 0 && nodes[1].charAt(0) != '"') {
                if (!nodes[1].startsWith(NodeWrapper.BNODE_SUBSTRING)) nsCounts.add(vault.store(NodeWrapper.getNs(nodes[1])));
            }
            if (nodes[2].length() > 0 && nodes[2].charAt(0) != '"') {
                if (!nodes[2].startsWith(NodeWrapper.BNODE_SUBSTRING)) nsCounts.add(vault.store(NodeWrapper.getNs(nodes[2])));
            }

			
		} else {
			System.out.println("Could not get triple from line. " + Arrays.toString(nodes));
		}

	}
	

	private BufferedReader getNtripleInputStream(File file) throws IOException {
		reader = null;
		if (file.getName().endsWith(".gz")) {
			fileStream = new FileInputStream(file);
			gzipStream = new GZIPInputStream(fileStream, 200536);//maximize buffer: http://java-performance.com/
			decoder = new InputStreamReader(gzipStream, "UTF-8");
			reader = new BufferedReader(decoder);
		} else {
			reader = new BufferedReader(new FileReader(file));
		}

		return reader;
	}

	private void close() throws IOException {
		if (gzipStream != null) gzipStream.close();
		if (fileStream != null) fileStream.close();
		if (decoder != null) decoder.close();
		if (reader != null) reader.close();

	}


	/**
	 * del delta. This way, when we re-run (forced) a dataset analysis, but we stop in the middle, we know we have to re-run this dataset again later on
	 * @throws IOException
	 */
	private void delDelta() throws IOException {
		File deltaFile = new File(datasetDir, StreamDatasetsNamespaces.DELTA_FILENAME);
		if (deltaFile.exists()) deltaFile.delete();
	}

	@Override
	public void run() {
		try {
			log("aggregating " + datasetDir.getName());
			delDelta();
			processDataset();
			StreamDatasetsNamespaces.PROCESSED_COUNT++;
			StreamDatasetsNamespaces.printProgress(datasetDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
