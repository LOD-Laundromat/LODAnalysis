package lodanalysis.model.statements;

import com.hp.hpl.jena.rdf.model.Property;

import lodanalysis.Paths;
import lodanalysis.model.CreateModel;
import lodanalysis.model.CreateModel.Namespace;

public class LengthUriSub extends DescriptiveStatsInfo{

	public LengthUriSub(CreateModel factory) {
		super(factory);
	}
	@Override
	protected Property getDescriptiveProp() {
		return getProp(Namespace.LLM, "subIRILength");
	}

	@Override
	protected String getMeanFilename() {
		return Paths.URI_SUB_LENGTH_AVG;
	}

	@Override
	protected String getMedianFilename() {
		return Paths.URI_SUB_LENGTH_MEDIAN;
	}

	@Override
	protected String getMinFilename() {
		return Paths.URI_SUB_LENGTH_MIN;
	}

	@Override
	protected String getMaxFilename() {
		return Paths.URI_SUB_LENGTH_MAX;
	}

	@Override
	protected String getStdFilename() {
		return Paths.URI_SUB_LENGTH_STD;
	}

}
