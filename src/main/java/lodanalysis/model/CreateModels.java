package lodanalysis.model;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import lodanalysis.Entry;
import lodanalysis.RuneableClass;
import lodanalysis.streamer.StreamDatasets;
import lodanalysis.utils.Utils;

import org.apache.commons.io.FileUtils;

public class CreateModels  extends RuneableClass{
	private static int DELTA_ID = 1;
	private static String DELTA_FILENAME = "description_delta";
	public CreateModels(Entry entry) throws IOException {
		super(entry);
		Set<File> metricDirs = entry.getMetricDirs();
		

		int totalCount = metricDirs.size();
		int processed = 0;
		for (File metricDir: metricDirs) {
			Utils.printProgress("creating descriptions", totalCount, processed);
			processed++;
			
			if (new File(metricDir, StreamDatasets.DELTA_FILENAME).exists() && //i.e., it has some files we can create the model from
					(entry.forceExec() || Utils.getDelta(metricDir, DELTA_FILENAME) < DELTA_ID)) {
				new CreateModel(metricDir);
				//write newest delta
				FileUtils.write(new File(metricDir, DELTA_FILENAME), Integer.toString(DELTA_ID));
			}
			
		}
		System.out.println();
		
	}
}