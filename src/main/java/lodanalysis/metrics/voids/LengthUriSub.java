package lodanalysis.metrics.voids;

import com.hp.hpl.jena.rdf.model.Property;

import lodanalysis.Paths;
import lodanalysis.metrics.DescriptionsFactory;
import lodanalysis.metrics.DescriptionsFactory.Namespace;

public class LengthUriSub extends DescriptiveStatsInfo{

	public LengthUriSub(DescriptionsFactory factory) {
		super(factory);
	}
	@Override
	protected Property getDescriptiveProp() {
		return getProp(Namespace.LLO, "subIRILength");
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
