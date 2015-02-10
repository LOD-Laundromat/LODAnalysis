package lodanalysis.metrics.voids;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;

import lodanalysis.Paths;
import lodanalysis.metrics.CreateModelStatement;
import lodanalysis.metrics.CreateModel;
import lodanalysis.metrics.CreateModel.Namespace;


public class NumDistinctLiterals extends CreateModelStatement {


	public NumDistinctLiterals(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		doc.addProperty(getProp(Namespace.LLM, "distinctLiterals"), FileUtils.readFileToString(new File(dir, Paths.DISTINCT_LITERALS)), XSDDatatype.XSDlong);
	}

}
