package lodanalysis.metrics.voids;

import com.hp.hpl.jena.rdf.model.Property;

import lodanalysis.Settings;
import lodanalysis.metrics.DescriptionsFactory;
import lodanalysis.metrics.DescriptionsFactory.Namespace;

public class Degree extends DegreeInfo{

	public Degree(DescriptionsFactory factory) {
		super(factory);
	}
	@Override
	protected Property getDegreeProp() {
		return getProp(Namespace.LL, "degree");
	}

	@Override
	protected String getMeanFilename() {
		return Settings.FILE_NAME_DEGREE_AVG;
	}

	@Override
	protected String getMedianFilename() {
		return Settings.FILE_NAME_DEGREE_MEDIAN;
	}

	@Override
	protected String getMinFilename() {
		return Settings.FILE_NAME_DEGREE_MIN;
	}

	@Override
	protected String getMaxFilename() {
		return Settings.FILE_NAME_DEGREE_MAX;
	}

	@Override
	protected String getStdFilename() {
		return Settings.FILE_NAME_DEGREE_STD;
	}

}
