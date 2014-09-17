package lodanalysis.metrics.voids;

import java.io.File;
import java.io.IOException;

import lodanalysis.Settings;
import lodanalysis.metrics.DescriptionCreator;
import lodanalysis.metrics.DescriptionsFactory;
import lodanalysis.metrics.DescriptionsFactory.Namespace;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;


public class NumDistinctProperties extends DescriptionCreator {


	public NumDistinctProperties(DescriptionsFactory factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		doc.addProperty(getProp(Namespace.VOID, "properties"), Integer.toString(countLines(new File(dir, Settings.FILE_NAME_PREDICATE_COUNTS))), XSDDatatype.XSDlong);
	}

}
