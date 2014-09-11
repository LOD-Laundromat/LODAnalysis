package lodanalysis.metrics;

import java.io.File;
import java.io.IOException;

import lodanalysis.Entry;
import lodanalysis.RuneableClass;
import lodanalysis.aggregator.Aggregator;
import lodanalysis.utils.Utils;

import org.apache.commons.io.FileUtils;

public class CreateDescriptions  extends RuneableClass{
	private static int DELTA_ID = 1;
	private static String DELTA_FILENAME = "aggregator_delta";
	public CreateDescriptions(Entry entry) throws IOException {
		super(entry);
		File[] metricDirs = entry.getMetricsDir().listFiles();
		

		int totalCount = metricDirs.length;
		int processed = 0;
		for (File metricDir: metricDirs) {
			Utils.printProgress("creating descriptions", totalCount, processed);
			processed++;
			
			if (new File(metricDir, Aggregator.DELTA_FILENAME).exists() && //i.e., it has some files we can create the model from
					(entry.forceExec() || Utils.getDelta(metricDir, DELTA_FILENAME) < DELTA_ID)) {
				new CreateModelFile(metricDir);
				//write newest delta
				FileUtils.write(new File(metricDir, DELTA_FILENAME), Integer.toString(DELTA_ID));
			}
			
		}
		System.out.println();
		
	}
}