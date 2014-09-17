package lodanalysis.metrics.voids;

import java.io.IOException;
import java.util.Properties;

import lodanalysis.metrics.DescriptionCreator;
import lodanalysis.metrics.DescriptionsFactory;
import lodanalysis.metrics.DescriptionsFactory.Namespace;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.rdf.model.Resource;


public class Provenance extends DescriptionCreator {


	public Provenance(DescriptionsFactory factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		
		Properties gitProps = new Properties();
		gitProps.load(getClass().getClassLoader().getResourceAsStream("git.properties"));
		
		
		/**
		 * Add provenance
		 */
		String gitUrl = gitProps.getProperty("git.remote.origin.url").toString().replace("git@", "https://");
		Resource provGitEntity = getResource(gitUrl + "#", gitProps.getProperty("git.commit.id").toString());
		provGitEntity.addProperty(getProp(Namespace.RDF, "type"), getResource(Namespace.PROV, "Entity"));
		provGitEntity.addProperty(getProp(Namespace.LLO, "gitId"), gitProps.getProperty("git.commit.id").toString(), XSDDatatype.XSDstring);
		provGitEntity.addProperty(getProp(Namespace.LLO, "gitBranch"), gitProps.getProperty("git.branch").toString(), XSDDatatype.XSDstring);
		provGitEntity.addProperty(getProp(Namespace.FOAF, "homePage"), "http://github.com/LODLaundromat/LODAnalysis", XSDDatatype.XSDstring);
		
		//define activity
		Resource provActivity = getBnode();
		provActivity.addProperty(getProp(Namespace.RDF, "type"), getResource(Namespace.PROV, "Activity"));
		provActivity.addProperty(getProp(Namespace.PROV, "used"), provGitEntity);
		
		//link activity with void document
		doc.addProperty(getProp(Namespace.RDF, "type"), getResource(Namespace.PROV, "Entity"));
		doc.addProperty(getProp(Namespace.PROV, "derivedFrom"), doc);
		doc.addProperty(getProp(Namespace.PROV, "generatedBy"), provActivity);
	}

}
