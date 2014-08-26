package lodanalysis.metrics;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import lodanalysis.Entry;
import lodanalysis.RuneableClass;
import lodanalysis.Settings;

import org.apache.commons.io.FileUtils;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

public class CreateDescriptions  extends RuneableClass{
	
	public static final String NS_LL = "http://lodlaundromat.org/vocab#";
	public static final String NS_VOID = "http://rdfs.org/ns/void#";
	public static final String NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	public static final String NS_RDFS = "http://www.w3.org/2000/01/rdf-schema#";
	public static final String NS_PROV = "http://www.w3.org/ns/prov#";
	public static final String NS_GIT = "http://todooooooo#";
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
	private Property gitCommit;
	private Property foafHomePage;
	private Property bio2RdfTripleCount;
	private Property bio22RdfUniqueSubjectCount;
	private Property bio22RdfUniquePredicateCount;
	private Property bio22RdfUniqueObjectCount;
	private Property bio22RdfTypeCount;
	private Property bio22RdfHasCount;
	private Property bio22RdfHasType;
	private Property bio22RdfPredicateObjectCount;
	private Property bio22RdfHasPredicate;
	private Property bio22RdfPredicateLiteralCount;
	
	
	private Resource doc;
	private Resource voidDoc;
	private Resource dsDescriptor;
	
	public CreateDescriptions(Entry entry) throws IOException {
		super(entry);
		
		for (File datasetDir: entry.getOutputDir().listFiles()) {
			/**
			 * Set Namespaces
			 */
			model = ModelFactory.createDefaultModel();
			model.setNsPrefix("ll", NS_LL);
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
			gitCommit = model.createProperty(NS_GIT, "commit");
			foafHomePage = model.createProperty(NS_FOAF, "homePage");
			bio2RdfTripleCount= model.createProperty(NS_DS, "has_triple_count");
			bio22RdfUniqueSubjectCount = model.createProperty(NS_DS, "has_unique_subject_count");
			bio22RdfUniquePredicateCount = model.createProperty(NS_DS, "has_unique_predicate_count");
			bio22RdfUniqueObjectCount = model.createProperty(NS_DS, "has_unique_object_count");
			bio22RdfTypeCount = model.createProperty(NS_DS, "has_type_count");
			bio22RdfHasCount = model.createProperty(NS_DS, "has_count");
			bio22RdfHasType = model.createProperty(NS_DS, "has_type");
			bio22RdfPredicateObjectCount = model.createProperty(NS_DS, "has_predicate_object_count");
			bio22RdfHasPredicate = model.createProperty(NS_DS, "has_predicate");
			bio22RdfPredicateLiteralCount = model.createProperty(NS_DS, "has_predicate_literal_count");

			/**
			 * Make links
			 */
			doc = model.createResource(NS_LL + datasetDir.getName());
			voidDoc = model.createResource(NS_LL + datasetDir.getName() + "-metrics");
			doc.addProperty(model.createProperty(NS_LL, "description"), voidDoc);
			voidDoc.addProperty(voidSubset, doc);
			dsDescriptor = model.createResource(NS_DS + "Dataset-Descriptor");
			
			
			voidDoc.addProperty(rdfType, model.createResource(NS_VOID + "Dataset"));
			String tripleCount = FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_TRIPLE_COUNT));
			voidDoc.addProperty(voidTriples, tripleCount, XSDDatatype.XSDlong);
			addBio2RdfSubsetProp(model.createResource(NS_DS + "Dataset-Triples"), tripleCount);
			
			voidDoc.addProperty(voidClasses, Integer.toString(countLines(new File(datasetDir, Settings.FILE_NAME_TYPE_COUNTS))), XSDDatatype.XSDinteger);
			
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
			 * create prop partitions
			 */
			for (String propLine: FileUtils.readLines(new File(datasetDir, Settings.FILE_NAME_PREDICATE_COUNTS))) {
				String[] propLineSplit = propLine.split("\\t");
				if (propLineSplit.length != 2) throw new IllegalStateException("Unexpected input. Cannot split: " + propLine);
				Resource bnode = model.createResource();
				bnode.addProperty(voidProperty, model.createResource(propLineSplit[0]));
				bnode.addProperty(voidEntities, propLineSplit[1], XSDDatatype.XSDinteger);
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
				bnode.addProperty(voidEntities, classLineSplit[1], XSDDatatype.XSDinteger);
				voidDoc.addProperty(voidClassPartition, bnode);
			}
			/**
			 * Add provenance
			 */
			//set git entity
			Resource provGitEntity = model.createResource("https://github.com/LODLaundry/LODAnalysis.git");
			provGitEntity.addProperty(gitCommit, "some long hash", XSDDatatype.XSDstring);
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
			
			
			model.write(System.out, "TTL");
			model.write(new FileOutputStream(new File(datasetDir, Settings.FILE_NAME_DESCRIPTION_TTL)), "TTL");
			System.exit(1);
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
}


//https://github.com/bio2rdf/bio2rdf-scripts/blob/master/statistics/bio2rdf_stats_virtuoso.php
//type counts (cheap)
//predicate literal count (cheap)
//predicate object (uri) count (cheap)
//get number of unique subjects and object literals for each predicate (expensive)
//get number of unique subjects and object IRIs for each predicate
//get the number of distinct subject and object types for each predicate (very expensive)