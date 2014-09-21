package lodanalysis.aggregator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lodanalysis.Entry;
import lodanalysis.RuneableClass;
import lodanalysis.Paths;
import lodanalysis.utils.Utils;

public class Aggregator  extends RuneableClass {
	public static int DELTA_ID = 2;//useful when we re-run code. We store this id in each directory. When we re-run a (possibly newer) dataset dir, we can check whether we should re-analyze this dir, or skip it
	public static String DELTA_FILENAME = "aggregator_delta";
	
	public static int TOTAL_DIR_COUNT;
	public static int PROCESSED_COUNT = 0;
	public static File PROVENANCE_FILE = new File(Paths.DIR_NAME_TMP + "/" + Paths.PROVENANCE);
	
	private static BufferedWriter LOG_FILE_WRITER;
	public Aggregator(Entry entry) throws IOException, InterruptedException {
		super(entry);
		/**
		 * initialize temp file containing all provenance. We'll copy this file next to every statistic file we generate (for provenance reasons)
		 */
		Utils.writeSystemInfoToFile(PROVENANCE_FILE);
		
		File logFile = new File(Paths.LOG_AGGREGATE);
		if (logFile.exists()) logFile.delete();
		LOG_FILE_WRITER = new BufferedWriter(new FileWriter(Paths.LOG_AGGREGATE));
		
		Collection<File> datasetDirs = entry.getDatasetDirs();
		TOTAL_DIR_COUNT = datasetDirs.size();
		int numThreads = entry.getNumThreads();
		
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		for (File datasetDir : datasetDirs) {
			if (entry.forceExec() || Utils.getDelta(new File(entry.getMetricsDir(), datasetDir.getName()), DELTA_FILENAME) < DELTA_ID) {
				Runnable worker = new AggregateDataset(entry, datasetDir);
				executor.execute(worker);
			} else {
				if (entry.isVerbose()) System.out.println("Skipping " + datasetDir.getName() + ". Already analyzed");
				PROCESSED_COUNT++;
				printProgress(datasetDir);//i.e., include the unprocessed datasets (which we already processed before) in the progress counter
				
			}
		}
		
		// This will make the executor accept no new threads
	    // and finish all existing threads in the queue
	    executor.shutdown();
	    // Wait until all threads are finish
	    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
	    
		System.out.println();
		LOG_FILE_WRITER.close();
		
	}
	
	public static void printProgress(File datasetDir) throws IOException {
		Utils.printProgress("aggregating", TOTAL_DIR_COUNT, PROCESSED_COUNT);
	}
	
	public static void writeToLogFile(String msg) throws IOException {
		LOG_FILE_WRITER.write(msg + "\n");
		LOG_FILE_WRITER.flush();
	}
	

}
