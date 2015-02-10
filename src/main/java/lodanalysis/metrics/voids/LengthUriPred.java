package lodanalysis.metrics.voids;

import com.hp.hpl.jena.rdf.model.Property;

import lodanalysis.Paths;
import lodanalysis.metrics.CreateModel;
import lodanalysis.metrics.CreateModel.Namespace;

public class LengthUriPred extends DescriptiveStatsInfo{

	public LengthUriPred(CreateModel factory) {
		super(factory);
	}
	@Override
	protected Property getDescriptiveProp() {
		return getProp(Namespace.LLM, "predIRILength");
	}

	@Override
	protected String getMeanFilename() {
		return Paths.URI_PRED_LENGTH_AVG;
	}

	@Override
	protected String getMedianFilename() {
		return Paths.URI_PRED_LENGTH_MEDIAN;
	}

	@Override
	protected String getMinFilename() {
		return Paths.URI_PRED_LENGTH_MIN;
	}

	@Override
	protected String getMaxFilename() {
		return Paths.URI_PRED_LENGTH_MAX;
	}

	@Override
	protected String getStdFilename() {
		return Paths.URI_PRED_LENGTH_STD;
	}

}
