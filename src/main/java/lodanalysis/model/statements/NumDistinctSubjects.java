package lodanalysis.model.statements;

import java.io.File;
import java.io.IOException;

import lodanalysis.Paths;
import lodanalysis.model.CreateModel;
import lodanalysis.model.CreateModelStatement;
import lodanalysis.model.CreateModel.Namespace;

import org.apache.commons.io.FileUtils;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;


public class NumDistinctSubjects extends CreateModelStatement {


	public NumDistinctSubjects(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		doc.addProperty(getProp(Namespace.VOID, "distinctSubjects"), FileUtils.readFileToString(new File(dir, Paths.DISTINCT_SUBJECTS)), XSDDatatype.XSDinteger);
	}

}
