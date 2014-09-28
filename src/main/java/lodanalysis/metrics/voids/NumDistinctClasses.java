package lodanalysis.metrics.voids;

import java.io.File;
import java.io.IOException;

import lodanalysis.Paths;
import lodanalysis.metrics.DescriptionCreator;
import lodanalysis.metrics.DescriptionsFactory;
import lodanalysis.metrics.DescriptionsFactory.Namespace;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;


public class NumDistinctClasses extends DescriptionCreator {


	public NumDistinctClasses(DescriptionsFactory factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		doc.addProperty(getProp(Namespace.LLM, "distinctClasses"), Integer.toString(countLines(new File(dir, Paths.CLASS_COUNTS))), XSDDatatype.XSDlong);
	}

}
