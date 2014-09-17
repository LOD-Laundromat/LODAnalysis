package lodanalysis.metrics.voids;

import com.hp.hpl.jena.rdf.model.Property;

import lodanalysis.Settings;
import lodanalysis.metrics.DescriptionsFactory;
import lodanalysis.metrics.DescriptionsFactory.Namespace;

public class DegreeIn extends DegreeInfo{

	public DegreeIn(DescriptionsFactory factory) {
		super(factory);
	}
	@Override
	protected Property getDegreeProp() {
		return getProp(Namespace.LL, "indegree");
	}

	@Override
	protected String getMeanFilename() {
		return Settings.FILE_NAME_INDEGREE_AVG;
	}

	@Override
	protected String getMedianFilename() {
		return Settings.FILE_NAME_INDEGREE_MEDIAN;
	}

	@Override
	protected String getMinFilename() {
		return Settings.FILE_NAME_INDEGREE_MIN;
	}

	@Override
	protected String getMaxFilename() {
		return Settings.FILE_NAME_INDEGREE_MAX;
	}

	@Override
	protected String getStdFilename() {
		return Settings.FILE_NAME_INDEGREE_STD;
	}

}
