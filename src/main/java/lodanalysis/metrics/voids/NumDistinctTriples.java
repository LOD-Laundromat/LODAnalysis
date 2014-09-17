package lodanalysis.metrics.voids;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;

import lodanalysis.Settings;
import lodanalysis.metrics.DescriptionCreator;
import lodanalysis.metrics.DescriptionsFactory;
import lodanalysis.metrics.DescriptionsFactory.Namespace;


public class NumDistinctTriples extends DescriptionCreator {


	public NumDistinctTriples(DescriptionsFactory factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		doc.addProperty(getProp(Namespace.VOID, "triples"), FileUtils.readFileToString(new File(dir, Settings.FILE_NAME_TRIPLE_COUNT)), XSDDatatype.XSDlong);
	}

}
