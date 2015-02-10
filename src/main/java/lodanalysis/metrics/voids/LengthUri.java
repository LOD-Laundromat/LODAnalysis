package lodanalysis.metrics.voids;

import com.hp.hpl.jena.rdf.model.Property;

import lodanalysis.Paths;
import lodanalysis.metrics.CreateModel;
import lodanalysis.metrics.CreateModel.Namespace;

public class LengthUri extends DescriptiveStatsInfo{

	public LengthUri(CreateModel factory) {
		super(factory);
	}
	@Override
	protected Property getDescriptiveProp() {
		return getProp(Namespace.LLM, "IRILength");
	}

	@Override
	protected String getMeanFilename() {
		return Paths.URI_LENGTH_AVG;
	}

	@Override
	protected String getMedianFilename() {
		return Paths.URI_LENGTH_MEDIAN;
	}

	@Override
	protected String getMinFilename() {
		return Paths.URI_LENGTH_MIN;
	}

	@Override
	protected String getMaxFilename() {
		return Paths.URI_LENGTH_MAX;
	}

	@Override
	protected String getStdFilename() {
		return Paths.URI_LENGTH_STD;
	}

}
