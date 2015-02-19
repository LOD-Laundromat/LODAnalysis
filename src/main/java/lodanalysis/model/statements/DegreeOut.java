package lodanalysis.model.statements;

import com.hp.hpl.jena.rdf.model.Property;

import lodanalysis.Paths;
import lodanalysis.model.CreateModel;
import lodanalysis.model.CreateModel.Namespace;

public class DegreeOut extends DescriptiveStatsInfo{

	public DegreeOut(CreateModel factory) {
		super(factory);
	}
	@Override
	protected Property getDescriptiveProp() {
		return getProp(Namespace.LLM, "outDegree");
	}

	@Override
	protected String getMeanFilename() {
		return Paths.OUTDEGREE_AVG;
	}

	@Override
	protected String getMedianFilename() {
		return Paths.OUTDEGREE_MEDIAN;
	}

	@Override
	protected String getMinFilename() {
		return Paths.OUTDEGREE_MIN;
	}

	@Override
	protected String getMaxFilename() {
		return Paths.OUTDEGREE_MAX;
	}

	@Override
	protected String getStdFilename() {
		return Paths.OUTDEGREE_STD;
	}

}
