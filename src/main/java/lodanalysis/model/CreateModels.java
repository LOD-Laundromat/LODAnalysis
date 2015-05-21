package lodanalysis.model;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lodanalysis.Entry;
import lodanalysis.RuneableClass;
import lodanalysis.streamer.StreamDatasets;
import lodanalysis.utils.Utils;

public class CreateModels  extends RuneableClass{
	private static int DELTA_ID = 1;
	private static String DELTA_FILENAME = "description_delta";
    public static int PROCESSED_COUNT = 0;
    public static int TOTAL_DIR_COUNT;
	public CreateModels(Entry entry) throws IOException {
		super(entry);
		Set<File> metricDirs = entry.getMetricDirs();
		

		TOTAL_DIR_COUNT = metricDirs.size();
		
		ExecutorService executor = Executors.newFixedThreadPool(entry.getNumThreads());
        for (File metricDir : metricDirs) {
            try {
                if (new File(metricDir, StreamDatasets.DELTA_FILENAME).exists() && //i.e., it has some files we can create the model from
                        (entry.forceExec() || Utils.getDelta(metricDir, DELTA_FILENAME) < DELTA_ID)) {
                    Runnable worker = new CreateModel(metricDir);
                    executor.execute(worker);
                } else {
                    PROCESSED_COUNT ++;
                    printProgress(metricDir);//i.e., include the unprocessed datasets (which we already processed before) in the progress counter
                        
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        

		
	}
    public static void printProgress(File datasetDir) throws IOException {
        Utils.printProgress("aggregating", TOTAL_DIR_COUNT, PROCESSED_COUNT);
    }
}