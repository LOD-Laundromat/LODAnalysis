package lodanalysis.streamer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import lodanalysis.Entry;
import lodanalysis.Paths;
import lodanalysis.utils.Utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.data2semantics.vault.PatriciaVault;
import org.data2semantics.vault.PatriciaVault.PatriciaNode;
import org.data2semantics.vault.Vault;

import com.google.common.collect.HashMultiset;

public class StreamDataset implements Runnable  {
	public class PredicateCounter {
	    
	    
	    
		int count = 1;//how often does this predicate occur. (initialize with 1)
		//how many literals (objects) does it co-occur with
		int objLiteralCount = 0;
		HashSet<PatriciaNode> distinctObjLiteralCount = new HashSet<PatriciaNode>();
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
	int uriCount = 0;
	int literalCount = 0;
	int distinctObjects = 0;
	int distinctNonLiteralObjects = 0;
	
	//private Set<PatriciaNode> binaryInfo = new HashSet<PatriciaNode>();
	//option: store fingerprint in single hashmap for 
	    //bnode/uri/literal, 
	    //class yes/no
	    //subject count
	    //predicate count
	    //object count
	    //definedsub/definedobj
	private Map<PatriciaNode, NodeWrapper> nodesInfo = new HashMap<PatriciaNode, NodeWrapper>();
	
	
	//this would reduce the number of hashmultisets we need (and greatly reduce memory)
//	private HashMultiset<PatriciaNode> bnodeCounts = HashMultiset.create();
//	private HashMultiset<PatriciaNode> subjects = null;
//	private HashMultiset<PatriciaNode> objects;
//	private HashMultiset<PatriciaNode> subUris = HashMultiset.create();
//	private HashMultiset<PatriciaNode> predUris = HashMultiset.create();
//	private HashMultiset<PatriciaNode> predBnodes = HashMultiset.create();
//	private HashMultiset<PatriciaNode> objUris = HashMultiset.create();
//	private HashMultiset<PatriciaNode> objBnodes = HashMultiset.create();
//	private HashMultiset<PatriciaNode> subBnodes = HashMultiset.create();
//	private HashMultiset<PatriciaNode> literals = HashMultiset.create();
//	private HashMultiset<PatriciaNode> classCounts = HashMultiset.create();
	
	
	private HashMultiset<PatriciaNode> nsCounts = HashMultiset.create();
	private HashMap<PatriciaNode, PredicateCounter> predicateCounts = new HashMap<PatriciaNode, PredicateCounter>();
//	private Set<PatriciaNode> distinctUris;
//	private Set<PatriciaNode> uriBnodeSet;

	
//	private Set<PatriciaNode> distinctLangTags = new HashSet<PatriciaNode>();
//	private Set<PatriciaNode> distinctDataTypes = new HashSet<PatriciaNode>();
//	private Set<PatriciaNode> distinctDefinedObjects = new HashSet<PatriciaNode>();
//	private Set<PatriciaNode> distinctDefinedProperties = new HashSet<PatriciaNode>();
//	private DescriptiveStatistics uriLengthStats;
//	private DescriptiveStatistics uriSubLengthStats = new DescriptiveStatistics();
//	private DescriptiveStatistics uriPredLengthStats = new DescriptiveStatistics();
//	private DescriptiveStatistics uriObjLengthStats = new DescriptiveStatistics();
//	private DescriptiveStatistics literalLengthStats = new DescriptiveStatistics();
//	private Set<PatriciaNode> distinctSos;
	
	
//	private NodeWrapper subWrapper = new NodeWrapper(vault);
//    private NodeWrapper predWrapper = new NodeWrapper(vault);
//    private NodeWrapper objWrapper = new NodeWrapper(vault);
	
	private Entry entry;
	public static void stream(Entry entry, File datasetDir) throws IOException {
		StreamDataset aggr = new StreamDataset(entry, datasetDir);

		aggr.run();
	}
	public StreamDataset(Entry entry, File datasetDir) throws IOException {
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
			if (!entry.datasetFitsSize(inputFile)) {
                System.err.println("Skipping because of file size: " + inputFile.toString());
                return;
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
    
    private NodeWrapper getAndSetNodeWrapper(PatriciaNode pnode, String stringRepresentation) {
        NodeWrapper nodeWrapper = nodesInfo.get(pnode);
        if (nodeWrapper == null) {
            nodeWrapper = new NodeWrapper(stringRepresentation);
            nodesInfo.put(pnode, nodeWrapper);
        }
        return nodeWrapper;
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
            PatriciaNode subTicket = vault.store(nodes[0]);
            PatriciaNode predTicket = vault.store(nodes[1]);
            PatriciaNode objTicket = vault.store(nodes[2]);
            
            NodeWrapper subWrapper = getAndSetNodeWrapper(subTicket, nodes[0]);
            NodeWrapper predWrapper = getAndSetNodeWrapper(predTicket, nodes[1]);
            NodeWrapper objWrapper = getAndSetNodeWrapper(objTicket, nodes[2]);
            
            subWrapper.update(Position.SUB);
            predWrapper.update(Position.PRED);
            objWrapper.update(Position.OBJ);
            
            /**
             * Some generic counters
             */
            tripleCount++;
            PredicateCounter predCounter = null;

            if (!predicateCounts.containsKey(predTicket)) {
                predCounter = new PredicateCounter();
                predicateCounts.put(predTicket, predCounter);
            } else {
                predCounter = predicateCounts.get(predTicket);
                predCounter.count++;
            }
            predCounter.distinctSubCount.add(subTicket);
            


            /**
             * store ns counters
             */
            if (subWrapper.type == Type.URI) nsCounts.add(subTicket);
            if (predWrapper.type == Type.URI) nsCounts.add(predTicket);
            if (objWrapper.type == Type.URI) nsCounts.add(objTicket);

            /**
             * Store literal info
             */
            if (objWrapper.type == Type.LITERAL) {
                literalCount++;
                predCounter.objLiteralCount++;
                predCounter.distinctObjLiteralCount.add(objTicket);
            } else {
                predCounter.objNonLiteralCount++;
                predCounter.distinctObjNonLiteralCount.add(objTicket);
            }
            
            /**
             * Store classes and props
             */
            if (predWrapper.isRdfType) {
                objWrapper.asTypeCount++;
                
                if (objWrapper.classType != null  && objWrapper.classType == ClassType.CLASS) {
                    subWrapper.definedAsClass = true;
                } else if (objWrapper.classType != null  && objWrapper.classType == ClassType.PROPERTY) {
                    subWrapper.definedAsProperty = true;
                }
            }
            
            
            
            
            //Store URI info
            if (subWrapper.type == Type.URI) {
                uriCount++;
            } 
            
            
            
            if (predWrapper.type == Type.URI) {
                uriCount++;
            }
            if (objWrapper.type == Type.URI) {
                uriCount++;
            }
        } else {
            System.out.println("Could not get triple from line. " + Arrays.toString(nodes));
        }

    }
	private void log(String msg) throws IOException {
		if (entry.isVerbose()) System.out.println(msg);
	}

	
	private void store() throws IOException {
	    
		File datasetOutputDir = entry.getMetricDirForMd5(Utils.pathToMd5(datasetDir));
		if (!datasetOutputDir.exists()) datasetOutputDir.mkdir();
		
		//store provenance file
        FileUtils.copyFile(StreamDatasets.PROVENANCE_FILE, new File(datasetOutputDir, ".sysinfo"));
        
        
		writePredCountersToFile(datasetOutputDir, predicateCounts);
		predicateCounts = null;
		
		
		Utils.writePatriciaCountsToFile(vault, new File(datasetOutputDir, Paths.NS_COUNTS), nsCounts);
		nsCounts = null;
		
		Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.ALL_LITERALS), literalCount);
		Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.ALL_URIS), uriCount);
		
		Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_TRIPLES), tripleCount);
		/**
		 * Now do tricky part, and post-process all the node info
		 */
		
		
		/**
		 * process all literals
		 */
		storeLiteralInfoAndMeasureCounts(datasetOutputDir);

		/**
		 * store sos and objects info. After this, we can remove literals from our nodes info thing, to reduce memory
		 */
		
		
		
		
		HashSet<PatriciaNode> distinctSos = new HashSet<PatriciaNode>();
		int allSubUriCount = 0;
		int allPredUriCount = 0;
		int allObjUriCount = 0;
		
		int distinctSubjects = 0;
		int distinctObjectsAndSubjects = 0;
		double[] indegree = new double[distinctNonLiteralObjects];
//		System.out.println(distinctNonLiteralObjects);
		int indegreeArrIndex = 0;
        for (Iterator<java.util.Map.Entry<PatriciaNode, NodeWrapper>> it = nodesInfo.entrySet().iterator(); it.hasNext();) {
            java.util.Map.Entry<PatriciaNode, NodeWrapper> entry = it.next();
            
            NodeWrapper nodeWrapper = entry.getValue();
            if (nodeWrapper.subCount > 0 || nodeWrapper.objCount > 0) distinctSos.add(entry.getKey());
            if (nodeWrapper.type != Type.LITERAL && nodeWrapper.objCount > 0) {
                indegree[indegreeArrIndex] = nodeWrapper.objCount;
                indegreeArrIndex++;
            }
            if (nodeWrapper.subCount > 0) {
                distinctSubjects++;
            } 
            if (nodeWrapper.type != Type.LITERAL && (nodeWrapper.subCount > 0 || nodeWrapper.objCount > 0)) distinctObjectsAndSubjects++;
            
            //we can remove literals from this hashmap now, to clean up memory
            if (nodeWrapper.type == Type.LITERAL) {
                vault.trash(entry.getKey());
                it.remove();
            } else if (nodeWrapper.type == Type.URI) {
                allSubUriCount += nodeWrapper.subCount;
                allPredUriCount += nodeWrapper.predCount;
                allObjUriCount += nodeWrapper.objCount;
            }
        }
		Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_SOS_COUNT), distinctSos.size());
		distinctSos = null;
		Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_OBJECTS), distinctObjects);
		Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_SUBJECTS), distinctSubjects);
        //indegree (media/median/mode/range)
        DescriptiveStatistics stats = new DescriptiveStatistics(indegree);
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.INDEGREE_AVG), stats.getMean());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.INDEGREE_MEDIAN), stats.getPercentile(50));
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.INDEGREE_STD), stats.getStandardDeviation());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.INDEGREE_MAX), stats.getMax());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.INDEGREE_MIN), stats.getMin());
		
		
		/**
		 * Process all the rest
		 */
		double[] allUriLengths = new double[allSubUriCount + allPredUriCount + allObjUriCount];
		int allUriLengthsNextIndex = 0;
		double[] allSubUriLengths = new double[allSubUriCount];
		int allSubUriLengthsNextIndex = 0;
		double[] allPredUriLengths = new double[allPredUriCount];
		int allPredUriLengthsNextIndex = 0;
		double[] allObjUriLengths = new double[allObjUriCount];
		int allObjUriLengthsNextIndex = 0;
		double[] outDegree = new double[distinctSubjects];
		int outDegreeNextIndex = 0;
		double[] degree = new double[distinctObjectsAndSubjects];
		int degreeNextIndex = 0;
		int distinctUris = 0;
		int distinctUrisSub = 0;//
		int distinctBnodesSub = 0;//
		int distinctUrisObj = 0;//
		int distinctBnodesObj = 0;//
		int distinctDefinedClasses = 0;
		int distinctDefinedProperties = 0;
		FileWriter bnodeCountsFw = new FileWriter(new File(datasetOutputDir, Paths.BNODE_COUNTS));
		FileWriter classCountsFw = new FileWriter(new File(datasetOutputDir, Paths.CLASS_COUNTS));
		FileOutputStream output = new FileOutputStream(new File(datasetOutputDir, Paths.URI_BNODE_SET));
        Writer resourcesFileFw = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8");
	        
	        
	        
		for (java.util.Map.Entry<PatriciaNode, NodeWrapper> entry : nodesInfo.entrySet()) {
		    NodeWrapper nodeWrapper = entry.getValue();
		    PatriciaNode pnode = entry.getKey();
		    String stringRepresentation = vault.redeem(pnode);
		    if (nodeWrapper.type == Type.URI) {
		        distinctUris++;
		        if (nodeWrapper.subCount > 0) {
		            distinctUrisSub++;
                }
                if (nodeWrapper.objCount > 0) {
                    distinctUrisObj++;
                }
		        
                for (int i = 0; i < nodeWrapper.subCount + nodeWrapper.predCount + nodeWrapper.objCount; i++) {
                    allUriLengths[allUriLengthsNextIndex] = stringRepresentation.length();
                    allUriLengthsNextIndex++;
                }
		        for (int i = 0; i < nodeWrapper.subCount; i++) {
		            allSubUriLengths[allSubUriLengthsNextIndex] = stringRepresentation.length();
		            allSubUriLengthsNextIndex++;
		        }
		        for (int i = 0; i < nodeWrapper.predCount; i++) {
		            allPredUriLengths[allPredUriLengthsNextIndex] = stringRepresentation.length();
		            allPredUriLengthsNextIndex++;
		        }
		        for (int i = 0; i < nodeWrapper.objCount; i++) {
		            allObjUriLengths[allObjUriLengthsNextIndex] = stringRepresentation.length();
		            allObjUriLengthsNextIndex++;
		            
		        }
		    } else if (nodeWrapper.type == Type.BNODE) {
		        
		        if (nodeWrapper.subCount > 0) {
		            distinctBnodesSub++;
                }
                if (nodeWrapper.objCount > 0) {
                    distinctBnodesObj++;
                }
		    }
		    if (nodeWrapper.definedAsClass) distinctDefinedClasses++;
		    if (nodeWrapper.definedAsProperty) distinctDefinedProperties++;
		    if (nodeWrapper.subCount > 0) {
		        outDegree[outDegreeNextIndex] = nodeWrapper.subCount;
		        outDegreeNextIndex++;
		    }
		    if (nodeWrapper.type != Type.LITERAL && (nodeWrapper.subCount > 0 || nodeWrapper.objCount > 0)) {
		        degree[degreeNextIndex] = nodeWrapper.subCount + nodeWrapper.objCount;
		        degreeNextIndex++;
		    }
		    if (nodeWrapper.type == Type.BNODE) {
		        bnodeCountsFw.write(stringRepresentation + "\t" + nodeWrapper.getNumOccurances() + System.getProperty("line.separator"));
		    }
		    if (nodeWrapper.asTypeCount > 0) {
		        classCountsFw.write(stringRepresentation + "\t" + nodeWrapper.getNumOccurances() + System.getProperty("line.separator"));
		    }
		    
		    if (nodeWrapper.type == Type.BNODE || nodeWrapper.type == Type.URI) {
		        resourcesFileFw.write(stringRepresentation + System.getProperty("line.separator"));
		    }
		}
		bnodeCountsFw.close();
		classCountsFw.close();
		resourcesFileFw.close();
		output.close();
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_URIS_SUB), distinctUrisSub);
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_BNODES_SUB), distinctBnodesSub);
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_URIS_OBJ), distinctUrisObj);
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_BNODES_OBJ), distinctBnodesObj);
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_DEFINED_CLASSES), distinctDefinedClasses);
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_DEFINED_PROPERTIES), distinctDefinedProperties);
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_URIS), distinctUris);
        
        stats = new DescriptiveStatistics(allUriLengths);
        allUriLengths = null;
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_LENGTH_AVG), stats.getMean());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_LENGTH_MEDIAN), stats.getPercentile(50));
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_LENGTH_STD), stats.getStandardDeviation());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_LENGTH_MAX), stats.getMax());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_LENGTH_MIN), stats.getMin());
		
        stats = new DescriptiveStatistics(allObjUriLengths);
        allObjUriLengths = null;
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_OBJ_LENGTH_AVG), stats.getMean());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_OBJ_LENGTH_MEDIAN), stats.getPercentile(50));
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_OBJ_LENGTH_STD), stats.getStandardDeviation());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_OBJ_LENGTH_MAX), stats.getMax());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_OBJ_LENGTH_MIN), stats.getMin());
        
        stats = new DescriptiveStatistics(allSubUriLengths);
        allSubUriLengths = null;
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_SUB_LENGTH_AVG), stats.getMean());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_SUB_LENGTH_MEDIAN), stats.getPercentile(50));
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_SUB_LENGTH_STD), stats.getStandardDeviation());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_SUB_LENGTH_MAX), stats.getMax());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_SUB_LENGTH_MIN), stats.getMin());
        
        stats = new DescriptiveStatistics(allPredUriLengths);
        allPredUriLengths = null;
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_PRED_LENGTH_AVG), stats.getMean());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_PRED_LENGTH_MEDIAN), stats.getPercentile(50));
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_PRED_LENGTH_STD), stats.getStandardDeviation());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_PRED_LENGTH_MAX), stats.getMax());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.URI_PRED_LENGTH_MIN), stats.getMin());
        
        stats = new DescriptiveStatistics(outDegree);
        outDegree = null;
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.OUTDEGREE_AVG), stats.getMean());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.OUTDEGREE_MEDIAN), stats.getPercentile(50));
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.OUTDEGREE_STD), stats.getStandardDeviation());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.OUTDEGREE_MAX), stats.getMax());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.OUTDEGREE_MIN), stats.getMin());
        
        
        stats = new DescriptiveStatistics(degree);
        degree = null;
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DEGREE_AVG), stats.getMean());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DEGREE_MEDIAN), stats.getPercentile(50));
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DEGREE_STD), stats.getStandardDeviation());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DEGREE_MAX), stats.getMax());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DEGREE_MIN), stats.getMin());
        
		

		/**
		 * Finally, store the delta of this run
		 */
//		FileUtils.copyFile(StreamDatasets.PROVENANCE_FILE, new File(nsTripleCountsFile.getAbsolutePath() + ".sysinfo"));
		FileUtils.write(new File(datasetOutputDir, StreamDatasets.DELTA_FILENAME), Integer.toString(StreamDatasets.DELTA_ID));
	}

	private void storeLiteralInfoAndMeasureCounts(File datasetOutputDir) throws IOException {
        int distinctLiterals = 0;
        Set<String> dataTypes = new HashSet<String>();
        Set<String> langTags = new HashSet<String>();
        double[] literalLengths = new double[literalCount];
        int nextIndex = 0;
        for (java.util.Map.Entry<PatriciaNode, NodeWrapper> entry : nodesInfo.entrySet()) {
            NodeWrapper nodeWrapper = entry.getValue();
            if (nodeWrapper.type == Type.LITERAL) {
                distinctLiterals++;
                String stringRepresentation = vault.redeem(entry.getKey());
                String dataType = Utils.getDataType(stringRepresentation);
                int dataTypeLength = 0;
                if (dataType != null) {
                    dataTypes.add(dataType);
                    dataTypeLength = dataType.length();
                }
                String langTag = Utils.getLangTag(stringRepresentation);
                int langTagLength = 0;
                if (langTag != null) {
                    langTags.add(langTag);
                    langTagLength = langTag.length();
                } 
                
                int literalLength = Utils.getLiteralLength(stringRepresentation, dataTypeLength, langTagLength);
                int numOccurances = nodeWrapper.getNumOccurances();
                for (int i = 0; i < numOccurances; i++) {
                    literalLengths[nextIndex++] = literalLength;
                }
            }
            if (nodeWrapper.objCount > 0) {
                distinctObjects++;
                if (nodeWrapper.type != Type.LITERAL) distinctNonLiteralObjects++;
            }
        }
        
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_DATA_TYPES), dataTypes.size());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_LANG_TAGS), langTags.size());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.DISTINCT_LITERALS), distinctLiterals);
        
        DescriptiveStatistics stats = new DescriptiveStatistics(literalLengths);
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.LITERAL_LENGTH_AVG), stats.getMean());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.LITERAL_LENGTH_MEDIAN), stats.getPercentile(50));
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.LITERAL_LENGTH_STD), stats.getStandardDeviation());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.LITERAL_LENGTH_MAX), stats.getMax());
        Utils.writeSingleCountToFile(new File(datasetOutputDir, Paths.LITERAL_LENGTH_MIN), stats.getMin());
    }

	


	private void writePredCountersToFile(File targetDir, HashMap<PatriciaNode, PredicateCounter> predCounters) throws IOException {
		File predCountsFile = new File(targetDir, Paths.PREDICATE_COUNTS);
		FileWriter fwPredCounts = new FileWriter(predCountsFile);
		File predLitCountFiles = new File(targetDir, Paths.PREDICATE_LITERAL_COUNTS);
		FileWriter fwPredLitCounts = new FileWriter(predLitCountFiles);
		File predUriCountsFile = new File(targetDir, Paths.PREDICATE_NON_LIT_COUNTS);
		FileWriter fwPredNonLitCounts = new FileWriter(predUriCountsFile);
		File predSubCountsFile = new File(targetDir, Paths.PREDICATE_SUB_COUNTS);
		FileWriter fwPredSubCountsFile = new FileWriter(predSubCountsFile);
		for (java.util.Map.Entry<PatriciaNode, PredicateCounter> entry: predCounters.entrySet()) {
			String pred = vault.redeem(entry.getKey());
			fwPredCounts.write(pred + "\t" + entry.getValue().count + System.getProperty("line.separator"));
			fwPredLitCounts.write(pred + "\t" + entry.getValue().objLiteralCount + "\t" + entry.getValue().distinctObjLiteralCount.size() + "\t" + System.getProperty("line.separator"));
			fwPredNonLitCounts.write(pred + "\t" + entry.getValue().objNonLiteralCount + "\t" + entry.getValue().distinctObjNonLiteralCount.size() + "\t" + System.getProperty("line.separator"));
			fwPredSubCountsFile.write(pred + "\t" + entry.getValue().distinctSubCount.size()  + System.getProperty("line.separator"));
		}
		fwPredCounts.close();
		fwPredLitCounts.close();
		fwPredNonLitCounts.close();
		fwPredSubCountsFile.close();
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
		File deltaFile = new File(datasetDir, StreamDatasets.DELTA_FILENAME);
		if (deltaFile.exists()) deltaFile.delete();
	}

	@Override
	public void run() {
		try {
			log("aggregating " + datasetDir.getName());
			delDelta();
			processDataset();
			StreamDatasets.PROCESSED_COUNT++;
			StreamDatasets.printProgress(datasetDir);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
