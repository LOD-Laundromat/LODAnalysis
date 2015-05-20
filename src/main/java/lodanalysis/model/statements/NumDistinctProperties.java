package lodanalysis.model.statements;

import java.io.File;
import java.io.IOException;

import lodanalysis.Paths;
import lodanalysis.model.CreateModel;
import lodanalysis.model.CreateModelStatement;
import lodanalysis.model.CreateModel.Namespace;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;


public class NumDistinctProperties extends CreateModelStatement {


	public NumDistinctProperties(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		doc.addProperty(getProp(Namespace.VOID, "properties"), Integer.toString(countLines(new File(dir, Paths.PREDICATE_COUNTS))), XSDDatatype.XSDlong);
	}

}
