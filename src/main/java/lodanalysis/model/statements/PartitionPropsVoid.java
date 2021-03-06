package lodanalysis.model.statements;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import lodanalysis.Paths;
import lodanalysis.model.CreateModel;
import lodanalysis.model.CreateModelStatement;
import lodanalysis.model.CreateModel.Namespace;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.rdf.model.Resource;


public class PartitionPropsVoid extends CreateModelStatement {


	public PartitionPropsVoid(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		for (String propLine: FileUtils.readLines(new File(dir, Paths.PREDICATE_COUNTS))) {
			String[] propLineSplit = propLine.split("\\t");
			if (propLineSplit.length != 2) throw new IllegalStateException("Unexpected input. Cannot split: " + propLine);
			Resource bnode = getBnode();
			bnode.addProperty(getProp(Namespace.VOID, "property"), factory.model.createResource(propLineSplit[0]));
			bnode.addProperty(getProp(Namespace.VOID, "entities"), propLineSplit[1], XSDDatatype.XSDinteger);
			doc.addProperty(getProp(Namespace.VOID, "propertyPartition"), bnode);
		}
	}

}
