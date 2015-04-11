package lodanalysis.streamer.singlerun;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import lodanalysis.Entry;
import lodanalysis.RuneableClass;
import lodanalysis.utils.Utils;

public class GetDatasetsResources  extends RuneableClass {
	private Collection<File> datasetDirs;
	public static int TOTAL_DIR_COUNT;
	public static int PROCESSED_COUNT = 0;
	
	public GetDatasetsResources(Entry entry) throws IOException, InterruptedException {
		super(entry);
		/**
		 * initialize temp file containing all provenance. We'll copy this file next to every statistic file we generate (for provenance reasons)
		 */
		
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
            GetDatasetResources stream = new GetDatasetResources(entry, datasetDir);
            stream.run();
	    }
        
    }

    private void runMultiThreaded(int numThreads) throws IOException, InterruptedException {
	    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (File datasetDir : datasetDirs) {
                Runnable worker = new GetDatasetResources(entry, datasetDir);
                executor.execute(worker);
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
