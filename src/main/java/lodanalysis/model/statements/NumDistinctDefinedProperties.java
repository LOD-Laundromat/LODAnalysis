package lodanalysis.model.statements;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import lodanalysis.Paths;
import lodanalysis.model.CreateModel;
import lodanalysis.model.CreateModelStatement;
import lodanalysis.model.CreateModel.Namespace;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;


public class NumDistinctDefinedProperties extends CreateModelStatement {


	public NumDistinctDefinedProperties(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		doc.addProperty(getProp(Namespace.LLM, "definedProperties"),FileUtils.readFileToString(new File(dir, Paths.DISTINCT_DEFINED_PROPERTIES)), XSDDatatype.XSDinteger);
	}

}
