package lodanalysis;

import java.io.IOException;



/**
 * just a class for debugging stuff in eclipse. Then I wont need to switch to cmd ;)
 */
public class Debug {
	
	
	
	
	public static void main(String[] args) throws IOException {

		/**
		 * ad hoc dataset resources script
		 */
//	    new Entry(new String[]{"-threads", "4","-datasets", "datasets", "-metrics", "metrics", "lodanalysis.streamer.StreamDatasetsLight"});
	    new Entry(new String[]{"-threads", "4","-datasets", "datasets", "-metrics", "metrics", "lodanalysis.streamer.singlerun.StreamDatasetsNamespaces"});
	    
		/**
		 * streamer
		 */
//		new Entry(new String[]{"-force","-dataset", "datasets/testDataset", "-metrics", "metrics", "lodanalysis.streamer.StreamDatasets"});

		/**
		 * model creator
		 */
		
//		new Entry(new String[]{"-force","-metrics", "metrics", "lodanalysis.model.CreateModels"});
		/**
		 * upload models
		 */
//		http://lodlaundromat.org#metrics-11
//		new Entry(new String[]{ "-force", "-metric", "metrics/0fb5cc2fb77fcd6c187ef3b4856f7813", "lodanalysis.model.StoreModelsInEndpoint"});
	}
}
