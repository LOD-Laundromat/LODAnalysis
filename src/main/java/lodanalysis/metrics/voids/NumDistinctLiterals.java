package lodanalysis.metrics.voids;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;

import lodanalysis.Paths;
import lodanalysis.metrics.DescriptionCreator;
import lodanalysis.metrics.DescriptionsFactory;
import lodanalysis.metrics.DescriptionsFactory.Namespace;


public class NumDistinctLiterals extends DescriptionCreator {


	public NumDistinctLiterals(DescriptionsFactory factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		doc.addProperty(getProp(Namespace.LLM, "distinctLiterals"), FileUtils.readFileToString(new File(dir, Paths.DISTINCT_LITERALS)), XSDDatatype.XSDlong);
	}

}
