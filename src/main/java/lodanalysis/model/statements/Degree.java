package lodanalysis.model.statements;

import com.hp.hpl.jena.rdf.model.Property;

import lodanalysis.Paths;
import lodanalysis.model.CreateModel;
import lodanalysis.model.CreateModel.Namespace;

public class Degree extends DescriptiveStatsInfo{

	public Degree(CreateModel factory) {
		super(factory);
	}
	@Override
	protected Property getDescriptiveProp() {
		return getProp(Namespace.LLM, "degree");
	}

	@Override
	protected String getMeanFilename() {
		return Paths.DEGREE_AVG;
	}

	@Override
	protected String getMedianFilename() {
		return Paths.DEGREE_MEDIAN;
	}

	@Override
	protected String getMinFilename() {
		return Paths.DEGREE_MIN;
	}

	@Override
	protected String getMaxFilename() {
		return Paths.DEGREE_MAX;
	}

	@Override
	protected String getStdFilename() {
		return Paths.DEGREE_STD;
	}

}
