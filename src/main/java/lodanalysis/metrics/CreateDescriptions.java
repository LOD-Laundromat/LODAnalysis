package lodanalysis.metrics;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

import lodanalysis.Entry;
import lodanalysis.RuneableClass;
import lodanalysis.Settings;
import lodanalysis.utils.Utils;

import org.apache.commons.io.FileUtils;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

public class CreateDescriptions  extends RuneableClass{
	private static Properties GIT_PROPS = null;
	public static final String NS_LL = "http://lodlaundromat.org/resource/";
	public static final String NS_LLO = "http://lodlaundromat.org/ontology/";
	public static final String NS_VOID = "http://rdfs.org/ns/void#";
	public static final String NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	public static final String NS_RDFS = "http://www.w3.org/2000/01/rdf-schema#";
	public static final String NS_PROV = "http://www.w3.org/ns/prov#";
	public static final String NS_FOAF = "http://xmlns.com/foaf/0.1/";
	public static final String NS_DS = "http://bio2rdf.org/bio2rdf.dataset_vocabulary:";
	
	private Model model;
	private Property rdfType;
	private Property rdfsSubclassOf;
	private Property voidTriples;
	private Property voidEntities;
	private Property voidDistinctEntities;
	private Property voidClasses;
	private Property voidProperties ;
	private Property voidDistinctSubjects;
	private Property voidDistinctObjects;
	private Property voidClassPartition;
	private Property voidPropertyPartition;
	private Property voidClass;
	private Property voidSubset;
	private Property voidProperty;
	private Property voidLinkPredicate;
	private Property voidObjectsTarget;
	private Property provDerivedFrom;
	private Property provGeneratedBy;
	private Property provUsed;
	private Property llGitId;
	private Property llGitBranch;
	private Property llOutDegree;
	private Property llInDegree;
	private Property llDegree;
	private Property llMean;
	private Property llStd;
	private Property llMedian;
	private Property llRange;
	private Property foafHomePage;
	
	private Resource doc;
	private Resource voidDoc;
	private Resource dsDescriptor;
	
	public CreateDescriptions(Entry entry) throws IOException {
		super(entry);
		
		File[] metricDirs = entry.getMetricsDir().listFiles();
		int totalCount = metricDirs.length;
		int processed = 0;
		for (File datasetDir: metricDirs) {
			Utils.printProgress("creating descriptions", totalCount, processed);
			processed++;
			/**
			 * Set Namespaces
			 */
			model = ModelFactory.createDefaultModel();
			model.setNsPrefix("ll", NS_LL);
			model.setNsPrefix("llo", NS_LLO);
			model.setNsPrefix("void", NS_VOID);
			model.setNsPrefix("rdf", NS_RDF);
			model.setNsPrefix("prov", NS_PROV);
			model.setNsPrefix("ds", NS_DS);
			
			/**
			 * Set properties
			 */
			rdfType = model.createProperty(NS_RDF, "type");
			rdfsSubclassOf = model.createProperty(NS_RDFS, "subClassOf");
			voidTriples = model.createProperty(NS_VOID, "triples");
			voidEntities = model.createProperty(NS_VOID, "entities");
			voidDistinctEntities = model.createProperty(NS_VOID, "distinctEntities");
			voidClasses = model.createProperty(NS_VOID, "classes");
			voidProperties  = model.createProperty(NS_VOID, "properties");
			voidDistinctSubjects = model.createProperty(NS_VOID, "distinctSubjects");
			voidDistinctObjects = model.createProperty(NS_VOID, "distinctObjects");
			voidClassPartition = model.createProperty(NS_VOID, "classPartition");
			voidPropertyPartition = model.createProperty(NS_VOID, "propertyPartition");
			voidSubset = model.createProperty(NS_VOID, "subset");
			voidClass = model.createProperty(NS_VOID, "class");
			voidProperty = model.createProperty(NS_VOID, "property");
			voidLinkPredicate = model.createProperty(NS_VOID, "linkPredicate");
			voidObjectsTarget = model.createProperty(NS_VOID, "objectsTarget");
			provDerivedFrom = model.createProperty(NS_PROV, "wasDerivedFrom");
			provGeneratedBy = model.createProperty(NS_PROV, "wasGeneratedBy");
			provUsed = model.createProperty(NS_PROV, "used");
			llGitId = model.createProperty(NS_LLO, "gitId");
			llGitBranch = model.createProperty(NS_LLO, "branch");
			llOutDegree = model.createProperty(NS_LLO, "outDegree");
			llInDegree = model.createProperty(NS_LLO, "inDegree");
			llDegree = model.createProperty(NS_LLO, "degree");
			llMean = model.createProperty(NS_LLO, "mean");
			llStd = model.createProperty(NS_LLO, "standardDeviation");
			llRange = model.createProperty(NS_LLO, "range");
			llMedian = model.createProperty(NS_LLO, "median");
			foafHomePage = model.createProperty(NS_FOAF, "homePage");

			/**
			 * Make links
			 */
			doc = model.createResource(NS_LL + datasetDir.getName());
			voidDoc = model.createResource(NS_LL + datasetDir.getName() + "/metrics");
			doc.addProperty(model.createProperty(NS_LLO, "metrics"), voidDoc);
			dsDescriptor = model.createResource(NS_DS + "Dataset-Descriptor");
			
			
			voidDoc.addProperty(rdfType, model.createResource(NS_VOID + "Dataset"));
			String tripleCount = FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_TRIPLE_COUNT));
			voidDoc.addProperty(voidTriples, tripleCount, XSDDatatype.XSDlong);
			addBio2RdfSubsetProp(model.createResource(NS_DS + "Dataset-Triples"), tripleCount);
			
			voidDoc.addProperty(voidClasses, Integer.toString(countLines(new File(datasetDir, Settings.FILE_NAME_TYPE_COUNTS))), XSDDatatype.XSDlong);
			
			String distinctPredicates = Integer.toString(countLines(new File(datasetDir, Settings.FILE_NAME_PREDICATE_COUNTS)));
			voidDoc.addProperty(voidProperties, distinctPredicates, XSDDatatype.XSDlong);
			addBio2RdfSubsetProp(model.createResource(NS_DS + "Dataset-Distinct-Properties"), distinctPredicates);
			
			String distinctObjects = FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_OBJECT_COUNT));
			voidDoc.addProperty(voidDistinctObjects, distinctObjects, XSDDatatype.XSDlong);
			addBio2RdfSubsetProp(model.createResource(NS_DS + "Dataset-Distinct-Objects"), distinctObjects);
			
			String distinctSubjects = FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_SUBJECT_COUNT));
			voidDoc.addProperty(voidDistinctSubjects, distinctSubjects, XSDDatatype.XSDlong);
			addBio2RdfSubsetProp(model.createResource(NS_DS + "Dataset-Distinct-Subjects"), distinctSubjects);
			
			String distinctEntities = FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_URI_COUNT));
			voidDoc.addProperty(voidEntities, distinctEntities, XSDDatatype.XSDlong);
			addBio2RdfSubsetProp(model.createResource(NS_DS + "Dataset-Distinct-Entities"), distinctEntities);
			
			addBio2RdfSubsetProp(model.createResource(NS_DS + "Dataset-Distinct-Literals"), FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_LITERAL_COUNT)));
			
			
			/**
			 * add degree information
			 */
			Resource inDegreeBnode = model.createResource();
			inDegreeBnode.addProperty(llMean, FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_INDEGREE_AVG)), XSDDatatype.XSDdouble);
			inDegreeBnode.addProperty(llMedian, FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_INDEGREE_MEDIAN)), XSDDatatype.XSDlong);
			inDegreeBnode.addProperty(llRange, FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_INDEGREE_RANGE)), XSDDatatype.XSDlong);
			inDegreeBnode.addProperty(llStd, FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_INDEGREE_STD)), XSDDatatype.XSDdouble);
			voidDoc.addProperty(llInDegree, inDegreeBnode);
			Resource outDegreeBnode = model.createResource();
			outDegreeBnode.addProperty(llMean, FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_OUTDEGREE_AVG)), XSDDatatype.XSDdouble);
			outDegreeBnode.addProperty(llMedian, FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_OUTDEGREE_MEDIAN)), XSDDatatype.XSDlong);
			outDegreeBnode.addProperty(llRange, FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_OUTDEGREE_RANGE)), XSDDatatype.XSDlong);
			outDegreeBnode.addProperty(llStd, FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_OUTDEGREE_STD)), XSDDatatype.XSDdouble);
			voidDoc.addProperty(llOutDegree, outDegreeBnode);
			Resource degreeBnode = model.createResource();
			degreeBnode.addProperty(llMean, FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_DEGREE_AVG)), XSDDatatype.XSDdouble);
			degreeBnode.addProperty(llMedian, FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_DEGREE_MEDIAN)), XSDDatatype.XSDlong);
			degreeBnode.addProperty(llRange, FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_DEGREE_RANGE)), XSDDatatype.XSDlong);
			degreeBnode.addProperty(llStd, FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_DEGREE_STD)), XSDDatatype.XSDdouble);
			voidDoc.addProperty(llDegree, degreeBnode);
			
			
			
			/**
			 * create prop partitions
			 */
			for (String propLine: FileUtils.readLines(new File(datasetDir, Settings.FILE_NAME_PREDICATE_COUNTS))) {
				String[] propLineSplit = propLine.split("\\t");
				if (propLineSplit.length != 2) throw new IllegalStateException("Unexpected input. Cannot split: " + propLine);
				Resource bnode = model.createResource();
				bnode.addProperty(voidProperty, model.createResource(propLineSplit[0]));
				bnode.addProperty(voidEntities, propLineSplit[1], XSDDatatype.XSDlong);
				voidDoc.addProperty(voidPropertyPartition, bnode);
			}
			/**
			 * create class partitions
			 */
			for (String classLine: FileUtils.readLines(new File(datasetDir, Settings.FILE_NAME_TYPE_COUNTS))) {
				String[] classLineSplit = classLine.split("\\t");
				if (classLineSplit.length != 2) throw new IllegalStateException("Unexpected input. Cannot split: " + classLine);
				Resource bnode = model.createResource();
				bnode.addProperty(voidClass, model.createResource(classLineSplit[0]));
				bnode.addProperty(voidEntities, classLineSplit[1], XSDDatatype.XSDlong);
				voidDoc.addProperty(voidClassPartition, bnode);
			}
			
			
			/**
			 * Add provenance
			 */
			String gitUrl = getGitProp("git.remote.origin.url").replace("git@", "https://");
			Resource provGitEntity = model.createResource(gitUrl + "#" + getGitProp("git.commit.id"));
			provGitEntity.addProperty(rdfType, model.createResource(NS_PROV + "Entity"));
			provGitEntity.addProperty(llGitId, getGitProp("git.commit.id"), XSDDatatype.XSDstring);
			provGitEntity.addProperty(llGitBranch, getGitProp("git.branch"), XSDDatatype.XSDstring);
			provGitEntity.addProperty(foafHomePage, "http://github.com/LODLaundromat/LODAnalysis", XSDDatatype.XSDstring);
			
			//define activity
			Resource provActivity = model.createResource();
			provActivity.addProperty(rdfType, model.createResource(NS_PROV + "Activity"));
			provActivity.addProperty(provUsed, provGitEntity);
			
			//link activity with void document
			voidDoc.addProperty(rdfType, model.createResource(NS_PROV + "Entity"));
			voidDoc.addProperty(provDerivedFrom, doc);
			voidDoc.addProperty(provGeneratedBy, provActivity);
			
			
			/**
			 * add bio2rdf metrics
			 * https://github.com/bio2rdf/bio2rdf-scripts/blob/master/statistics/bio2rdf_stats_virtuoso.php
			 * https://github.com/bio2rdf/bio2rdf-scripts/wiki/Bio2RDF-dataset-metrics
			 */
			//add type counts: how often is each type used
			for (String typeLine: FileUtils.readLines(new File(datasetDir, Settings.FILE_NAME_TYPE_COUNTS))) {
				String[] typeLineSplit = typeLine.split("\\t");
				if (typeLineSplit.length != 2) throw new IllegalStateException("Unexpected input. Cannot split: " + typeLine);
				
				Resource bnode = model.createResource();
				bnode.addProperty(rdfType, model.createResource(NS_DS + "Dataset-Type-Count"));
				bnode.addProperty(voidClass, model.createResource(typeLineSplit[0]));
				bnode.addProperty(voidEntities, typeLineSplit[1], XSDDatatype.XSDlong);
				voidDoc.addProperty(voidSubset, bnode);
			}
			
			
			//object (URI) property counts. How often does a predicate co-occur with a non-literal in object position
			for (String predLine: FileUtils.readLines(new File(datasetDir, Settings.FILE_NAME_PREDICATE_NON_LIT_COUNTS))) {
				String[] predLineSplit = predLine.split("\\t");
				if (predLineSplit.length != 3) throw new IllegalStateException("Unexpected input. Cannot split: " + predLine);
				
				Resource voidSubsetBnode = model.createResource();
				voidSubsetBnode.addProperty(rdfType, model.createResource(NS_DS + "Dataset-Object-Property-Count"));
				voidSubsetBnode.addProperty(voidLinkPredicate, model.createResource(predLineSplit[0]));
				
				Resource voidObjectsTargetBnode = model.createResource();
				voidObjectsTargetBnode.addProperty(voidEntities, predLineSplit[1], XSDDatatype.XSDlong);
				voidObjectsTargetBnode.addProperty(voidDistinctEntities, predLineSplit[2], XSDDatatype.XSDlong);
				voidSubsetBnode.addProperty(voidObjectsTarget, voidObjectsTargetBnode);
				
				voidDoc.addProperty(voidSubset, voidSubsetBnode);
			}
			
			//datatype (literal) property counts. How often does a predicate co-occur with a literal in object position
			for (String predLine: FileUtils.readLines(new File(datasetDir, Settings.FILE_NAME_PREDICATE_LITERAL_COUNTS))) {
				String[] predLineSplit = predLine.split("\\t");
				if (predLineSplit.length != 3) throw new IllegalStateException("Unexpected input. Cannot split: " + predLine);
				
				Resource voidSubsetBnode = model.createResource();
				voidSubsetBnode.addProperty(rdfType, model.createResource(NS_DS + "Dataset-Datatype-Property-Count"));
				voidSubsetBnode.addProperty(voidLinkPredicate, model.createResource(predLineSplit[0]));
				
				Resource voidObjectsTargetBnode = model.createResource();
				voidObjectsTargetBnode.addProperty(voidEntities, predLineSplit[1], XSDDatatype.XSDlong);
				voidObjectsTargetBnode.addProperty(voidDistinctEntities, predLineSplit[2], XSDDatatype.XSDlong);
				voidSubsetBnode.addProperty(voidObjectsTarget, voidObjectsTargetBnode);
				
				voidDoc.addProperty(voidSubset, voidSubsetBnode);
			}
			
			
//			model.write(System.out, "TTL");
			model.write(new FileOutputStream(new File(datasetDir, Settings.FILE_NAME_DESCRIPTION_TTL)), "TTL");
		}
	}
	
	
	/**
	 * pretty fast implementation, taken from http://stackoverflow.com/questions/453018/number-of-lines-in-a-file-in-java
	 * @param filename
	 * @return
	 * @throws IOException
	 */
	private int countLines(File file) throws IOException {
	    InputStream is = new BufferedInputStream(new FileInputStream(file));
	    try {
	        byte[] c = new byte[1024];
	        int count = 0;
	        int readChars = 0;
	        boolean empty = true;
	        while ((readChars = is.read(c)) != -1) {
	            empty = false;
	            for (int i = 0; i < readChars; ++i) {
	                if (c[i] == '\n') {
	                    ++count;
	                }
	            }
	        }
	        return (count == 0 && !empty) ? 1 : count;
	    } finally {
	        is.close();
	    }
	}
	
	private Resource addBio2RdfSubsetProp(Resource bio2rdfMetricType, String count) {
		Resource bnode = model.createResource();
		bnode.addProperty(rdfType, bio2rdfMetricType);
		bnode.addProperty(voidEntities, count, XSDDatatype.XSDlong);
		voidDoc.addProperty(voidSubset, bnode);
		bio2rdfMetricType.addProperty(rdfsSubclassOf, dsDescriptor);
		return bnode;
	}
	
	private String getGitProp(String prop) throws IOException {
		if (GIT_PROPS == null) {
			GIT_PROPS = new Properties();
			GIT_PROPS.load(getClass().getClassLoader().getResourceAsStream("git.properties"));
		}
		return GIT_PROPS.get(prop).toString();
	}
	
}


