package lodanalysis.metrics.voids;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;

import lodanalysis.Paths;
import lodanalysis.metrics.CreateModelStatement;
import lodanalysis.metrics.CreateModel;
import lodanalysis.metrics.CreateModel.Namespace;


public class NumDistinctNodes extends CreateModelStatement {


	public NumDistinctNodes(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		int totalNodes = Integer.parseInt(FileUtils.readFileToString(new File(dir, Paths.DISTINCT_LITERALS))) +
				Integer.parseInt(FileUtils.readFileToString(new File(dir, Paths.DISTINCT_URIS))) + 
				countLines(new File(dir, Paths.BNODE_COUNTS));
		doc.addProperty(getProp(Namespace.LLM, "distinctRDFNodes"), Integer.toString(totalNodes), XSDDatatype.XSDlong);
	}

}
