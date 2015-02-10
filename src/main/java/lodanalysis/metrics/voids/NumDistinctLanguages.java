package lodanalysis.metrics.voids;

import java.io.File;
import java.io.IOException;

import lodanalysis.Paths;
import lodanalysis.metrics.CreateModelStatement;
import lodanalysis.metrics.CreateModel;
import lodanalysis.metrics.CreateModel.Namespace;

import org.apache.commons.io.FileUtils;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;


public class NumDistinctLanguages extends CreateModelStatement {


	public NumDistinctLanguages(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		doc.addProperty(getProp(Namespace.LLM, "distinctLanguages"), FileUtils.readFileToString(new File(dir, Paths.DISTINCT_LANG_TAGS)), XSDDatatype.XSDlong);
	}

}
