package lodanalysis.metrics.voids;

import com.hp.hpl.jena.rdf.model.Property;

import lodanalysis.Paths;
import lodanalysis.metrics.DescriptionsFactory;
import lodanalysis.metrics.DescriptionsFactory.Namespace;

public class LengthLiteral extends DescriptiveStatsInfo{

	public LengthLiteral(DescriptionsFactory factory) {
		super(factory);
	}
	@Override
	protected Property getDescriptiveProp() {
		return getProp(Namespace.LLM, "literalLength");
	}

	@Override
	protected String getMeanFilename() {
		return Paths.LITERAL_LENGTH_AVG;
	}

	@Override
	protected String getMedianFilename() {
		return Paths.LITERAL_LENGTH_MEDIAN;
	}

	@Override
	protected String getMinFilename() {
		return Paths.LITERAL_LENGTH_MIN;
	}

	@Override
	protected String getMaxFilename() {
		return Paths.LITERAL_LENGTH_MAX;
	}

	@Override
	protected String getStdFilename() {
		return Paths.LITERAL_LENGTH_STD;
	}

}
