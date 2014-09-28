package lodanalysis.utils;

import java.io.IOException;

import lodanalysis.Entry;



/**
 * just a class for debugging stuff in eclipse. Then I wont need to switch to cmd ;)
 */
public class Debug {
	
	
	
	
	public static void main(String[] args) throws IOException {

		
		
		//java -jar -path <path> lodanalysis.aggregator.Aggregator
		/**
		 * aggregator
		 */
		new Entry(new String[]{"-nostrict", "-data_version", "11",  "-threads", "1","-datasets", "datasets", "-metrics", "output", "lodanalysis.aggregator.Aggregator"});
		/**
		 * create void descriptions
		 */
//		new Entry(new String[]{"-force", "-data_version", "11", "-nostrict","-datasets", "datasets", "-metrics", "output", "lodanalysis.metrics.CreateDescriptions"});
		/**
		 * store void descriptions
		 */
//		new Entry(new String[]{"-data_version", "11", "-nostrict", "-threads", "1","-datasets", "datasets", "-metrics", "output", "lodanalysis.metrics.StoreDescriptionsInEndpoint"});
	}
}
