package lodanalysis.aggregator;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import lodanalysis.Entry;
import lodanalysis.RuneableClass;

import org.apache.commons.io.FileUtils;

public class Aggregator  extends RuneableClass {
	private static int DELTA_ID = 1;//useful when we re-run code. We store this id in each directory. When we re-run a (possibly newer) dataset dir, we can check whether we should re-analyze this dir, or skip it
	private static String DELTA_FILENAME = "aggregator_delta";

	public Aggregator(Entry entry) throws IOException {
		super(entry);
		Collection<File> datasetDirs = entry.getDatasetDirs();
		int totalDirCount = datasetDirs.size();
		int count = 0;
		for (File datasetDir: datasetDirs) {
			String percentage = (String.format("%.0f%%",(100 * (float)count) / (float) totalDirCount));
			System.out.print("aggregating (" + percentage + ") " + getDatasetName(datasetDir) + "\r");
			if (entry.forceExec() || getDelta(datasetDir) < DELTA_ID) {
				AggregateDataset.aggregate(entry, datasetDir);
				storeDelta(datasetDir);
			} else {
				if (entry.isVerbose()) System.out.println("Skipping " + datasetDir.getName() + ". Already analyzed");
			}
			count++;
		}
		System.out.println();
	}

	private void storeDelta(File datasetDir) throws IOException {
		File deltaFile = new File(datasetDir, DELTA_FILENAME);
		FileUtils.write(deltaFile, Integer.toString(DELTA_ID));
	}
	private int getDelta(File datasetDir) throws IOException {
		int delta = -1;
		File deltaFile = new File(datasetDir, DELTA_FILENAME);
		if (deltaFile.exists()) {
			delta = Integer.parseInt(FileUtils.readFileToString(deltaFile).trim());
		}
		return delta;
	}

	private String getDatasetName(File datasetDir) throws IOException {
		String name = "";
		File basenameFile = new File(datasetDir, "basename");
		if (basenameFile.exists()) name = FileUtils.readFileToString(basenameFile).trim();
		return name;
	}


}
