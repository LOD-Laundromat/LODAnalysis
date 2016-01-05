package lodanalysis.model.statements;

import java.io.File;
import java.io.IOException;

import lodanalysis.Paths;
import lodanalysis.model.CreateModel;
import lodanalysis.model.CreateModelStatement;
import lodanalysis.model.CreateModel.Namespace;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;


public class NumDistinctBnodes extends CreateModelStatement {


	public NumDistinctBnodes(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		doc.addProperty(getProp(Namespace.VOID_EXT, "distinctBlankNodes"), Integer.toString(countLines(new File(dir, Paths.BNODE_COUNTS))), XSDDatatype.XSDinteger);
	}

}
