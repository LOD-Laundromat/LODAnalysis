package lodanalysis.streamer.singlerun;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import lodanalysis.Entry;
import lodanalysis.Paths;

import org.apache.commons.io.FileUtils;
import org.data2semantics.vault.PatriciaVault;
import org.data2semantics.vault.PatriciaVault.PatriciaNode;
import org.data2semantics.vault.Vault;

public class GetDatasetResources implements Runnable  {
	

	private File datasetDir;
	private InputStream gzipStream;
	private InputStream fileStream;
	private Reader decoder;
	private BufferedReader reader;
	private Vault<String, PatriciaNode> vault =  new PatriciaVault();
	private boolean isNquadFile = false;
	
	
	private Set<PatriciaNode> distinctUris = new HashSet<PatriciaNode>();
	private Set<PatriciaNode> distinctSos = new HashSet<PatriciaNode>();
	
	private Entry entry;
	public static void stream(Entry entry, File datasetDir) throws IOException {
		GetDatasetResources aggr = new GetDatasetResources(entry, datasetDir);

		aggr.run();
	}
	public GetDatasetResources(Entry entry, File datasetDir) throws IOException {
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
		
		File writeFile = new File(datasetOutputDir, Paths.URI_BNODE_SET);
		if (writeFile.exists()) writeFile.delete();
		FileOutputStream output = new FileOutputStream(new File(datasetOutputDir, Paths.URI_BNODE_SET));
        Writer resourcesFileFw = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8");
        for (PatriciaNode pNode : distinctUris) {
            resourcesFileFw.write(vault.redeem(pNode) + System.getProperty("line.separator"));
        }
        resourcesFileFw.close();
        output.close();
        FileUtils.writeStringToFile(new File(datasetOutputDir, Paths.DISTINCT_SOS_COUNT), Integer.toString(distinctSos.size()));
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
		    
		    PatriciaNode sub = vault.store(nodes[0]);
		    PatriciaNode pred = vault.store(nodes[1]);
		    PatriciaNode obj = vault.store(nodes[2]);
		    //get distinct uris
		    if (nodes[0].length() > 0 && nodes[0].charAt(0) != '"') {
		        distinctUris.add(sub);
		    }
		    if (nodes[1].length() > 0 && nodes[1].charAt(0) != '"') {
		        distinctUris.add(pred);
		    }
		    if (nodes[2].length() > 0 && nodes[2].charAt(0) != '"') {
		        distinctUris.add(obj);
		    }
		    distinctSos.add(sub);
		    distinctSos.add(obj);
		    
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



	@Override
	public void run() {
		try {
			log("aggregating " + datasetDir.getName());
			processDataset();
			GetDatasetsResources.PROCESSED_COUNT++;
			GetDatasetsResources.printProgress(datasetDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
