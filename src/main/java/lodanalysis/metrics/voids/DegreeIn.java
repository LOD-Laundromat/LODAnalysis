package lodanalysis.metrics.voids;

import com.hp.hpl.jena.rdf.model.Property;

import lodanalysis.Paths;
import lodanalysis.metrics.DescriptionsFactory;
import lodanalysis.metrics.DescriptionsFactory.Namespace;

public class DegreeIn extends DescriptiveStatsInfo{

	public DegreeIn(DescriptionsFactory factory) {
		super(factory);
	}
	@Override
	protected Property getDescriptiveProp() {
		return getProp(Namespace.LLO, "indegree");
	}

	@Override
	protected String getMeanFilename() {
		return Paths.INDEGREE_AVG;
	}

	@Override
	protected String getMedianFilename() {
		return Paths.INDEGREE_MEDIAN;
	}

	@Override
	protected String getMinFilename() {
		return Paths.INDEGREE_MIN;
	}

	@Override
	protected String getMaxFilename() {
		return Paths.INDEGREE_MAX;
	}

	@Override
	protected String getStdFilename() {
		return Paths.INDEGREE_STD;
	}

}
