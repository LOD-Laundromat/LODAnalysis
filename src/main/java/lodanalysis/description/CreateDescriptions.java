package lodanalysis.description;

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
	public static final String NS_PROV = "http://www.w3.org/ns/prov#";
	public static final String NS_GIT = "http://todooooooo#";
	public static final String NS_FOAF = "http://xmlns.com/foaf/0.1/";
	public CreateDescriptions(Entry entry) throws IOException {
		super(entry);
		
		for (File datasetDir: entry.getOutputDir().listFiles()) {
			/**
			 * Set Namespaces
			 */
			Model model = ModelFactory.createDefaultModel();
			model.setNsPrefix("ll", NS_LL);
			model.setNsPrefix("void", NS_VOID);
			model.setNsPrefix("rdf", NS_RDF);
			model.setNsPrefix("prov", NS_PROV);
			
			/**
			 * Set properties
			 */
			Property type = model.createProperty(NS_RDF, "type");
			Property voidTriples = model.createProperty(NS_VOID, "triples");
			Property voidEntities = model.createProperty(NS_VOID, "entities");
			Property voidClasses = model.createProperty(NS_VOID, "classes");
			Property voidProperties  = model.createProperty(NS_VOID, "properties");
			Property voidDistinctSubjects = model.createProperty(NS_VOID, "distinctSubjects");
			Property voidDistinctObjects = model.createProperty(NS_VOID, "distinctObjects");
			Property voidClassPartition = model.createProperty(NS_VOID, "classPartition");
			Property voidPropertyPartition = model.createProperty(NS_VOID, "propertyPartition");
			Property voidClass = model.createProperty(NS_VOID, "class");
			Property voidProperty = model.createProperty(NS_VOID, "property");
			Property provDerivedFrom = model.createProperty(NS_PROV, "wasDerivedFrom");
			Property provGeneratedBy = model.createProperty(NS_PROV, "wasGeneratedBy");
			Property provUsed = model.createProperty(NS_PROV, "used");
			Property gitCommit = model.createProperty(NS_GIT, "commit");
			Property foafHomePage = model.createProperty(NS_FOAF, "homePage");
			/**
			 * Make links
			 */
			Resource doc = model.createResource(NS_LL + datasetDir.getName());
			Resource voidDoc = model.createResource(NS_LL + datasetDir.getName() + "-descr");
			doc.addProperty(model.createProperty(NS_LL, "description"), voidDoc);
			
			voidDoc.addProperty(type, model.createResource(NS_VOID + "Dataset"));
			voidDoc.addProperty(voidTriples, FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_TRIPLE_COUNT)), XSDDatatype.XSDinteger);
			voidDoc.addProperty(voidClasses, Integer.toString(countLines(new File(datasetDir, Settings.FILE_NAME_CLASS_COUNTS))), XSDDatatype.XSDinteger);
			voidDoc.addProperty(voidProperties, Integer.toString(countLines(new File(datasetDir, Settings.FILE_NAME_PREDICATE_COUNTS))), XSDDatatype.XSDinteger);
			voidDoc.addProperty(voidDistinctObjects, FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_OBJECT_COUNT)), XSDDatatype.XSDinteger);
			voidDoc.addProperty(voidDistinctSubjects, FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_SUBJECT_COUNT)), XSDDatatype.XSDinteger);
			voidDoc.addProperty(voidEntities, FileUtils.readFileToString(new File(datasetDir, Settings.FILE_NAME_URI_COUNT)), XSDDatatype.XSDinteger);
			
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
			for (String classLine: FileUtils.readLines(new File(datasetDir, Settings.FILE_NAME_CLASS_COUNTS))) {
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
			provActivity.addProperty(type, model.createResource(NS_PROV + "Activity"));
			provActivity.addProperty(provUsed, provGitEntity);
			
			//link activity with void document
			voidDoc.addProperty(type, model.createResource(NS_PROV + "Entity"));
			voidDoc.addProperty(provDerivedFrom, doc);
			voidDoc.addProperty(provGeneratedBy, provActivity);
			
			
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
}

