package lodanalysis.metrics.voids;

import java.io.File;
import java.io.IOException;

import lodanalysis.Paths;
import lodanalysis.metrics.DescriptionCreator;
import lodanalysis.metrics.DescriptionsFactory;
import lodanalysis.metrics.DescriptionsFactory.Namespace;

import org.apache.commons.io.FileUtils;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;


public class NumDistinctEntities extends DescriptionCreator {


	public NumDistinctEntities(DescriptionsFactory factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		doc.addProperty(getProp(Namespace.LLM, "distinctEntities"), FileUtils.readFileToString(new File(dir, Paths.DISTINCT_URIS)), XSDDatatype.XSDlong);
	}

}
