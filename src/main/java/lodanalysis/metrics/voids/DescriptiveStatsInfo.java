package lodanalysis.metrics.voids;

import java.io.File;
import java.io.IOException;

import lodanalysis.metrics.CreateModelStatement;
import lodanalysis.metrics.CreateModel;
import lodanalysis.metrics.CreateModel.Namespace;

import org.apache.commons.io.FileUtils;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;


public abstract class DescriptiveStatsInfo extends CreateModelStatement {

	public DescriptiveStatsInfo(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		Resource descriptiveStats = getBnode();
		descriptiveStats.addProperty(getProp(Namespace.LLM, "mean"), FileUtils.readFileToString(new File(dir, getMeanFilename())), XSDDatatype.XSDdouble);
		descriptiveStats.addProperty(getProp(Namespace.LLM, "median"), FileUtils.readFileToString(new File(dir, getMedianFilename())), XSDDatatype.XSDlong);
		descriptiveStats.addProperty(getProp(Namespace.LLM, "min"), FileUtils.readFileToString(new File(dir, getMinFilename())), XSDDatatype.XSDlong);
		descriptiveStats.addProperty(getProp(Namespace.LLM, "max"), FileUtils.readFileToString(new File(dir, getMaxFilename())), XSDDatatype.XSDlong);
		descriptiveStats.addProperty(getProp(Namespace.LLM, "std"), FileUtils.readFileToString(new File(dir, getStdFilename())), XSDDatatype.XSDdouble);
		doc.addProperty(getDescriptiveProp(), descriptiveStats);
	}
	
	protected abstract Property getDescriptiveProp();
	
	protected abstract String getMeanFilename();
	protected abstract String getMedianFilename();
	protected abstract String getMinFilename();
	protected abstract String getMaxFilename();
	protected abstract String getStdFilename();
}
