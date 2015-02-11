package lodanalysis.model.statements;

import com.hp.hpl.jena.rdf.model.Property;

import lodanalysis.Paths;
import lodanalysis.model.CreateModel;
import lodanalysis.model.CreateModel.Namespace;

public class LengthUriObj extends DescriptiveStatsInfo{

	public LengthUriObj(CreateModel factory) {
		super(factory);
	}
	@Override
	protected Property getDescriptiveProp() {
		return getProp(Namespace.LLM, "objIRILength");
	}

	@Override
	protected String getMeanFilename() {
		return Paths.URI_OBJ_LENGTH_AVG;
	}

	@Override
	protected String getMedianFilename() {
		return Paths.URI_OBJ_LENGTH_MEDIAN;
	}

	@Override
	protected String getMinFilename() {
		return Paths.URI_OBJ_LENGTH_MIN;
	}

	@Override
	protected String getMaxFilename() {
		return Paths.URI_OBJ_LENGTH_MAX;
	}

	@Override
	protected String getStdFilename() {
		return Paths.URI_OBJ_LENGTH_STD;
	}

}
