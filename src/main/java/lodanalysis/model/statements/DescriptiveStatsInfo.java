package lodanalysis.model.statements;

import java.io.File;
import java.io.IOException;

import lodanalysis.model.CreateModel;
import lodanalysis.model.CreateModelStatement;
import lodanalysis.model.CreateModel.Namespace;

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
	    String mean = FileUtils.readFileToString(new File(dir, getMeanFilename()));
	    if (mean.equals("NaN")) return;//We don't have descriptive stats. (i.e. the n was zero). Just don't store this
		Resource descriptiveStats = getBnode();
		descriptiveStats.addProperty(getProp(Namespace.LLM, "mean"), mean, XSDDatatype.XSDdouble);
		descriptiveStats.addProperty(getProp(Namespace.LLM, "median"), FileUtils.readFileToString(new File(dir, getMedianFilename())), XSDDatatype.XSDinteger);
		descriptiveStats.addProperty(getProp(Namespace.LLM, "min"), FileUtils.readFileToString(new File(dir, getMinFilename())), XSDDatatype.XSDinteger);
		descriptiveStats.addProperty(getProp(Namespace.LLM, "max"), FileUtils.readFileToString(new File(dir, getMaxFilename())), XSDDatatype.XSDinteger);
		descriptiveStats.addProperty(getProp(Namespace.LLM, "std"), FileUtils.readFileToString(new File(dir, getStdFilename())), XSDDatatype.XSDdouble);
		descriptiveStats.addProperty(getProp(Namespace.RDF, "type"), getResource(Namespace.LLM, "DescriptiveStatistics"));
		doc.addProperty(getDescriptiveProp(), descriptiveStats);
	}
	
	protected abstract Property getDescriptiveProp();
	
	protected abstract String getMeanFilename();
	protected abstract String getMedianFilename();
	protected abstract String getMinFilename();
	protected abstract String getMaxFilename();
	protected abstract String getStdFilename();
}
