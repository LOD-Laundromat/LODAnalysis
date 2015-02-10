package lodanalysis.metrics.voids;

import java.io.File;
import java.io.IOException;

import lodanalysis.Paths;
import lodanalysis.metrics.CreateModelStatement;
import lodanalysis.metrics.CreateModel;
import lodanalysis.metrics.CreateModel.Namespace;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;


public class NumDistinctDefinedProperties extends CreateModelStatement {


	public NumDistinctDefinedProperties(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		doc.addProperty(getProp(Namespace.LLM, "distinctDefinedProperties"), Integer.toString(countLines(new File(dir, Paths.DISTINCT_DEFINED_PROPERTIES))), XSDDatatype.XSDlong);
	}

}
