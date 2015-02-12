package lodanalysis;

import java.io.IOException;



/**
 * just a class for debugging stuff in eclipse. Then I wont need to switch to cmd ;)
 */
public class Debug {
	
	
	
	
	public static void main(String[] args) throws IOException {

		
		
		/**
		 * streamer
		 */
//		new Entry(new String[]{"-force","-dataset", "datasets/testDataset", "-metrics", "metrics", "lodanalysis.streamer.StreamDatasets"});
		new Entry(new String[]{"-force","-datasets", "datasets", "-metrics", "metrics", "lodanalysis.streamer.StreamDatasets"});
		//java -jar /home/lrd900/code/lodAnalysis/target/lodAnalysis-1.0-SNAPSHOT-jar-with-dependencies.jar   -threads 20 -datasets datasets/ -metrics metrics/ "lodanalysis.streamer.StreamDatasets"

		/**
		 * model creator
		 */
		
//		new Entry(new String[]{"-force","-metrics", "metrics", "lodanalysis.model.CreateModels"});
		/**
		 * upload models
		 */
//		http://lodlaundromat.org#metrics-11
//		new Entry(new String[]{"-data_version", "11", "-nostrict", "-threads", "1","-datasets", "datasets", "-metrics", "metrics", "lodanalysis.metrics.StoreDescriptionsInEndpoint"});
	}
}
