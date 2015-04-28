package lodanalysis.streamer.singlerun;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lodanalysis.Entry;
import lodanalysis.Paths;
import lodanalysis.RuneableClass;
import lodanalysis.utils.Utils;

public class StreamDatasetsNamespaces  extends RuneableClass {
	public static int DELTA_ID = 2;//useful when we re-run code. We store this id in each directory. When we re-run a (possibly newer) dataset dir, we can check whether we should re-analyze this dir, or skip it
	public static String DELTA_FILENAME = "aggregator_delta";
	private Collection<File> datasetDirs;
	public static int TOTAL_DIR_COUNT;
	public static int PROCESSED_COUNT = 0;
	public static File PROVENANCE_FILE = null;;
	
	public StreamDatasetsNamespaces(Entry entry) throws IOException, InterruptedException {
		super(entry);
		/**
		 * initialize temp file containing all provenance. We'll copy this file next to every statistic file we generate (for provenance reasons)
		 */
		if (PROVENANCE_FILE == null) PROVENANCE_FILE = new File(entry.getTmpDir(), Paths.PROVENANCE);
		Utils.writeSystemInfoToFile(PROVENANCE_FILE);
		
		datasetDirs = entry.getDatasetDirs();
		TOTAL_DIR_COUNT = datasetDirs.size();
		int numThreads = entry.getNumThreads();
		
		if (numThreads > 1) {
		    runMultiThreaded(numThreads);
		} else {
		    runSingleThread();
		}
		
		
		
	    
		System.out.println();
		
	}
	
	private void runSingleThread() throws IOException {
	    for (File datasetDir : datasetDirs) {
		printProgress(datasetDir);
	        if (entry.forceExec() || !(new File(entry.getMetricParentDir().getPath() + "/" + datasetDir.getName() + "/" + Paths.NS_COUNTS).exists())) {
	            StreamDatasetNamespaces stream = new StreamDatasetNamespaces(entry, datasetDir);
	            stream.run();
	        } else {
	            if (entry.isVerbose()) System.out.println("Skipping " + datasetDir.getName() + ". Already analyzed");
                PROCESSED_COUNT++;
                printProgress(datasetDir);//i.e., include the unprocessed datasets (which we already processed before) in the progress counter
	        }
	    }
        
    }

    private void runMultiThreaded(int numThreads) throws IOException, InterruptedException {
	    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (File datasetDir : datasetDirs) {
            if (entry.forceExec() || !(new File(entry.getMetricParentDir().getPath() + "/" + datasetDir.getName() + "/" + Paths.NS_COUNTS).exists())) {
                Runnable worker = new StreamDatasetNamespaces(entry, datasetDir);
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
        
    }

    public static void printProgress(File datasetDir) throws IOException {
		Utils.printProgress("aggregating", TOTAL_DIR_COUNT, PROCESSED_COUNT);
	}
	

}
