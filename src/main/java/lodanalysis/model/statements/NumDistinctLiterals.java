package lodanalysis.model.statements;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;

import lodanalysis.Paths;
import lodanalysis.model.CreateModel;
import lodanalysis.model.CreateModelStatement;
import lodanalysis.model.CreateModel.Namespace;


public class NumDistinctLiterals extends CreateModelStatement {


	public NumDistinctLiterals(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		doc.addProperty(getProp(Namespace.VOID_EXT, "distinctLiterals"), FileUtils.readFileToString(new File(dir, Paths.DISTINCT_LITERALS)), XSDDatatype.XSDlong);
	}

}
