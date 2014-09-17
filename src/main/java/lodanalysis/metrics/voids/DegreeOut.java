package lodanalysis.metrics.voids;

import com.hp.hpl.jena.rdf.model.Property;

import lodanalysis.Settings;
import lodanalysis.metrics.DescriptionsFactory;
import lodanalysis.metrics.DescriptionsFactory.Namespace;

public class DegreeOut extends DegreeInfo{

	public DegreeOut(DescriptionsFactory factory) {
		super(factory);
	}
	@Override
	protected Property getDegreeProp() {
		return getProp(Namespace.LL, "outdegree");
	}

	@Override
	protected String getMeanFilename() {
		return Settings.FILE_NAME_OUTDEGREE_AVG;
	}

	@Override
	protected String getMedianFilename() {
		return Settings.FILE_NAME_OUTDEGREE_MEDIAN;
	}

	@Override
	protected String getMinFilename() {
		return Settings.FILE_NAME_OUTDEGREE_MIN;
	}

	@Override
	protected String getMaxFilename() {
		return Settings.FILE_NAME_OUTDEGREE_MAX;
	}

	@Override
	protected String getStdFilename() {
		return Settings.FILE_NAME_OUTDEGREE_STD;
	}

}
