package lodanalysis.metrics;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import lodanalysis.Paths;
import lodanalysis.metrics.voids.Degree;
import lodanalysis.metrics.voids.DegreeIn;
import lodanalysis.metrics.voids.DegreeOut;
import lodanalysis.metrics.voids.LengthLiteral;
import lodanalysis.metrics.voids.LengthUri;
import lodanalysis.metrics.voids.LengthUriObj;
import lodanalysis.metrics.voids.LengthUriPred;
import lodanalysis.metrics.voids.LengthUriSub;
import lodanalysis.metrics.voids.NumDistinctBnodes;
import lodanalysis.metrics.voids.NumDistinctClasses;
import lodanalysis.metrics.voids.NumDistinctDefinedClasses;
import lodanalysis.metrics.voids.NumDistinctDefinedProperties;
import lodanalysis.metrics.voids.NumDistinctEntities;
import lodanalysis.metrics.voids.NumDistinctLiterals;
import lodanalysis.metrics.voids.NumDistinctObjects;
import lodanalysis.metrics.voids.NumDistinctProperties;
import lodanalysis.metrics.voids.NumDistinctSubjects;
import lodanalysis.metrics.voids.NumDistinctTriples;
import lodanalysis.metrics.voids.PartitionPropsVoid;
import lodanalysis.metrics.voids.PartitionTypesDs;
import lodanalysis.metrics.voids.PartitionTypesVoid;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;

public class DescriptionsFactory {
	public enum Namespace {
		LL("ll", "http://lodlaundromat.org/resource/"),
		LLO("llo", "http://lodlaundromat.org/ontology/"),
		LLM("llm", "http://lodlaundromat.org/metrics/ontology/"),
		VOID("void", "http://rdfs.org/ns/void#"),
		VOID_EXT("void-ext", "http://ldf.fi/void-ext#"),
		RDF("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
		RDFS("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
		PROV("prov", "http://www.w3.org/ns/prov#"),
		FOAF("foaf", "http://xmlns.com/foaf/0.1/"),
		DS("ds", "http://bio2rdf.org/bio2rdf.dataset_vocabulary:");
		
		private String prefix;
		private String url;

		Namespace(String prefix, String url) {
			this.prefix = prefix;
			this.url = url;
		}
		public String getPrefix() {
			return prefix;
		}
		public String getUrl() {
			return url;
		}
	}
	
	public Model model;
	public Resource voidDoc;
	public File metricDir;
	
	

	

	public DescriptionsFactory(File metricDir) throws IOException {
		this.metricDir = metricDir;
		model = ModelFactory.createDefaultModel();
		for (Namespace ns: Namespace.values()) {
			model.setNsPrefix(ns.getPrefix(), ns.getUrl());
		}

		voidDoc = model.createResource(Namespace.LL.getUrl() + metricDir.getName() + "/metrics");
		model.createResource(Namespace.LL.getUrl() + metricDir.getName()).addProperty(model.createProperty(Namespace.LLM.getUrl(), "metrics"), voidDoc);
		voidDoc.addProperty(model.createProperty(Namespace.RDF.getUrl(), "type"), model.createResource(Namespace.DS + "Dataset"));
		
		DescriptionCreator[] descriptions = new DescriptionCreator[] {
				new NumDistinctTriples(this),
				new NumDistinctBnodes(this),
				new NumDistinctClasses(this),
				new NumDistinctEntities(this),
				new NumDistinctLiterals(this),
				new NumDistinctObjects(this),
				new NumDistinctProperties(this),
				new NumDistinctSubjects(this),
				new NumDistinctDefinedClasses(this),
				new NumDistinctDefinedProperties(this),
				new PartitionPropsVoid(this),
				new PartitionTypesDs(this),
				new PartitionTypesVoid(this),
				new Degree(this),
				new DegreeIn(this),
				new DegreeOut(this),
				new LengthUri(this),
				new LengthUriObj(this),
				new LengthUriPred(this),
				new LengthUriSub(this),
				new LengthLiteral(this),
		};
		for (DescriptionCreator description: descriptions) {
			description.createDescription();
		}
		
		
		model.write(new FileOutputStream(new File(metricDir, Paths.DESCRIPTION_NT)), "NT");
	}
	
	
}


