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
import lodanalysis.Settings;
import lodanalysis.utils.Utils;

import org.apache.commons.io.FileUtils;

public class Aggregator  extends RuneableClass {
	public static int DELTA_ID = 1;//useful when we re-run code. We store this id in each directory. When we re-run a (possibly newer) dataset dir, we can check whether we should re-analyze this dir, or skip it
	public static String DELTA_FILENAME = "aggregator_delta";
	public static int TOTAL_DIR_COUNT;
	public static int PROCESSED_COUNT = 0;
	
	
	private static BufferedWriter LOG_FILE_WRITER;
	public Aggregator(Entry entry) throws IOException, InterruptedException {
		super(entry);
		File logFile = new File(Settings.FILE_NAME_LOG_AGGREGATE);
		if (logFile.exists()) logFile.delete();
		LOG_FILE_WRITER = new BufferedWriter(new FileWriter(Settings.FILE_NAME_LOG_AGGREGATE));
		
		Collection<File> datasetDirs = entry.getDatasetDirs();
		TOTAL_DIR_COUNT = datasetDirs.size();
		int numThreads = entry.getNumThreads();
		
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		for (File datasetDir : datasetDirs) {
			if (entry.forceExec() || getDelta(datasetDir) < DELTA_ID) {
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
		String percentage = (String.format("%.0f%%",(100 * (float)PROCESSED_COUNT) / (float) TOTAL_DIR_COUNT));
		System.out.print("aggregating (" + percentage + ") " + Utils.getDatasetName(datasetDir) + "\r");
	}
	
	public static void writeToLogFile(String msg) throws IOException {
		LOG_FILE_WRITER.write(msg + "\n");
		LOG_FILE_WRITER.flush();
	}
	
	private int getDelta(File datasetDir) throws IOException {
		int delta = -1;
		File deltaFile = new File(datasetDir, DELTA_FILENAME);
		if (deltaFile.exists()) {
			delta = Integer.parseInt(FileUtils.readFileToString(deltaFile).trim());
		}
		return delta;
	}




}
