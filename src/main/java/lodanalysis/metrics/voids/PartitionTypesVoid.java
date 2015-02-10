package lodanalysis.metrics.voids;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import lodanalysis.Paths;
import lodanalysis.metrics.CreateModelStatement;
import lodanalysis.metrics.CreateModel;
import lodanalysis.metrics.CreateModel.Namespace;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.rdf.model.Resource;


public class PartitionTypesVoid extends CreateModelStatement {


	public PartitionTypesVoid(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		for (String classLine: FileUtils.readLines(new File(dir, Paths.CLASS_COUNTS))) {
			String[] classLineSplit = classLine.split("\\t");
			if (classLineSplit.length != 2) throw new IllegalStateException("Unexpected input. Cannot split: " + classLine);
			Resource bnode = getBnode();
			bnode.addProperty(getProp(Namespace.VOID, "class"), factory.model.createResource(classLineSplit[0]));
			bnode.addProperty(getProp(Namespace.VOID, "entities"), classLineSplit[1], XSDDatatype.XSDlong);
			doc.addProperty(getProp(Namespace.VOID, "classPartition"), bnode);
		}
	}

}
