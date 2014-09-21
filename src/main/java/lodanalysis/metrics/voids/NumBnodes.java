package lodanalysis.metrics.voids;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import lodanalysis.Paths;
import lodanalysis.metrics.DescriptionCreator;
import lodanalysis.metrics.DescriptionsFactory;
import lodanalysis.metrics.DescriptionsFactory.Namespace;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;


public class NumBnodes extends DescriptionCreator {


	public NumBnodes(DescriptionsFactory factory) {
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
		
		doc.addProperty(getProp(Namespace.LLO, "blankNodes"), Integer.toString(bnodeCount), XSDDatatype.XSDlong);
	}

}
