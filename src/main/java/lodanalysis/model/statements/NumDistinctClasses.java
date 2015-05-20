package lodanalysis.model.statements;

import java.io.File;
import java.io.IOException;

import lodanalysis.Paths;
import lodanalysis.model.CreateModel;
import lodanalysis.model.CreateModelStatement;
import lodanalysis.model.CreateModel.Namespace;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;


public class NumDistinctClasses extends CreateModelStatement {


	public NumDistinctClasses(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		doc.addProperty(getProp(Namespace.VOID, "classes"), Integer.toString(countLines(new File(dir, Paths.CLASS_COUNTS))), XSDDatatype.XSDlong);
	}

}
