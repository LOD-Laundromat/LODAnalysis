package lodanalysis.metrics.voids;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import lodanalysis.Settings;
import lodanalysis.metrics.DescriptionCreator;
import lodanalysis.metrics.DescriptionsFactory;
import lodanalysis.metrics.DescriptionsFactory.Namespace;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;


public abstract class DegreeInfo extends DescriptionCreator {
	protected enum Stats {
		MEAN(Namespace.LL + "mean", Settings.FILE_NAME_INDEGREE_AVG);
		Stats(String ns, String filename) {
			
		}
	}

	public DegreeInfo(DescriptionsFactory factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		Resource inDegreeBnode = getBnode();
		inDegreeBnode.addProperty(getProp(Namespace.LL, "mean"), FileUtils.readFileToString(new File(dir, getMeanFilename())), XSDDatatype.XSDdouble);
		inDegreeBnode.addProperty(getProp(Namespace.LL, "median"), FileUtils.readFileToString(new File(dir, getMedianFilename())), XSDDatatype.XSDlong);
		inDegreeBnode.addProperty(getProp(Namespace.LL, "min"), FileUtils.readFileToString(new File(dir, getMinFilename())), XSDDatatype.XSDlong);
		inDegreeBnode.addProperty(getProp(Namespace.LL, "max"), FileUtils.readFileToString(new File(dir, getMaxFilename())), XSDDatatype.XSDlong);
		inDegreeBnode.addProperty(getProp(Namespace.LL, "std"), FileUtils.readFileToString(new File(dir, getStdFilename())), XSDDatatype.XSDdouble);
		doc.addProperty(getDegreeProp(), inDegreeBnode);
	}
	
	protected abstract Property getDegreeProp();
	
	protected abstract String getMeanFilename();
	protected abstract String getMedianFilename();
	protected abstract String getMinFilename();
	protected abstract String getMaxFilename();
	protected abstract String getStdFilename();
}
