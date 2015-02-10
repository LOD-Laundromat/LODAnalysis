package lodanalysis.metrics.voids;

import java.io.File;
import java.io.IOException;

import lodanalysis.Paths;
import lodanalysis.metrics.CreateModelStatement;
import lodanalysis.metrics.CreateModel;
import lodanalysis.metrics.CreateModel.Namespace;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;


public class NumDistinctDefinedClasses extends CreateModelStatement {


	public NumDistinctDefinedClasses(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		doc.addProperty(getProp(Namespace.LLM, "distinctDefinedClasses"), Integer.toString(countLines(new File(dir, Paths.DISTINCT_DEFINED_CLASSES))), XSDDatatype.XSDlong);
	}

}
