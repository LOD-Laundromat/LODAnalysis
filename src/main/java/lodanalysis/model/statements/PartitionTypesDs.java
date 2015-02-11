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


public class PartitionTypesDs extends CreateModelStatement {


	public PartitionTypesDs(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
		/**
		 * add bio2rdf metrics
		 * https://github.com/bio2rdf/bio2rdf-scripts/blob/master/statistics/bio2rdf_stats_virtuoso.php
		 * https://github.com/bio2rdf/bio2rdf-scripts/wiki/Bio2RDF-dataset-metrics
		 */
		//add type counts: how often is each type used
		for (String typeLine: FileUtils.readLines(new File(dir, Paths.CLASS_COUNTS))) {
			String[] typeLineSplit = typeLine.split("\\t");
			if (typeLineSplit.length != 2) throw new IllegalStateException("Unexpected input. Cannot split: " + typeLine);
			
			Resource bnode = getBnode();
			bnode.addProperty(getProp(Namespace.RDF, "type"), getResource(Namespace.DS, "Dataset-Type-Count"));
			bnode.addProperty(getProp(Namespace.VOID, "class"), factory.model.createResource(typeLineSplit[0]));
			bnode.addProperty(getProp(Namespace.VOID, "entities"), typeLineSplit[1], XSDDatatype.XSDlong);
			doc.addProperty(getProp(Namespace.VOID, "subset"), bnode);
		}
	}

}
