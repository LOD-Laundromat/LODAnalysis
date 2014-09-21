package lodanalysis.metrics.voids;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import lodanalysis.Paths;
import lodanalysis.metrics.DescriptionCreator;
import lodanalysis.metrics.DescriptionsFactory;
import lodanalysis.metrics.DescriptionsFactory.Namespace;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;


public abstract class DescriptiveStatsInfo extends DescriptionCreator {

	public DescriptiveStatsInfo(DescriptionsFactory factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		Resource descriptiveStats = getBnode();
		descriptiveStats.addProperty(getProp(Namespace.LLO, "mean"), FileUtils.readFileToString(new File(dir, getMeanFilename())), XSDDatatype.XSDdouble);
		descriptiveStats.addProperty(getProp(Namespace.LLO, "median"), FileUtils.readFileToString(new File(dir, getMedianFilename())), XSDDatatype.XSDlong);
		descriptiveStats.addProperty(getProp(Namespace.LLO, "min"), FileUtils.readFileToString(new File(dir, getMinFilename())), XSDDatatype.XSDlong);
		descriptiveStats.addProperty(getProp(Namespace.LLO, "max"), FileUtils.readFileToString(new File(dir, getMaxFilename())), XSDDatatype.XSDlong);
		descriptiveStats.addProperty(getProp(Namespace.LLO, "std"), FileUtils.readFileToString(new File(dir, getStdFilename())), XSDDatatype.XSDdouble);
		doc.addProperty(getDescriptiveProp(), descriptiveStats);
	}
	
	protected abstract Property getDescriptiveProp();
	
	protected abstract String getMeanFilename();
	protected abstract String getMedianFilename();
	protected abstract String getMinFilename();
	protected abstract String getMaxFilename();
	protected abstract String getStdFilename();
}
