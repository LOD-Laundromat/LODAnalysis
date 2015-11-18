package lodanalysis.model.statements;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import lodanalysis.Paths;
import lodanalysis.model.CreateModel;
import lodanalysis.model.CreateModelStatement;
import lodanalysis.model.CreateModel.Namespace;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;


public class NumBnodes extends CreateModelStatement {


	public NumBnodes(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		int bnodeCount = 0;
		for (String bnodeLine: FileUtils.readLines(new File(dir, Paths.BNODE_COUNTS))) {
			String[] typeLineSplit = bnodeLine.split("\\t");
			if (typeLineSplit.length != 2) throw new IllegalStateException("Unexpected input. Cannot split: " + bnodeLine);
			bnodeCount += Integer.parseInt(typeLineSplit[1]);
		}
		
		doc.addProperty(getProp(Namespace.LLM, "blankNodes"), Integer.toString(bnodeCount), XSDDatatype.XSDinteger);
	}

}
