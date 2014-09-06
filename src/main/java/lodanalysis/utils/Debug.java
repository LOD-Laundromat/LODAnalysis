package lodanalysis.utils;

import lodanalysis.Entry;



/**
 * just a class for debugging stuff in eclipse. Then I wont need to switch to cmd ;)
 */
public class Debug {
	
	
	
	
	public static void main(String[] args) {

		
		
		//java -jar -path <path> lodanalysis.aggregator.Aggregator
		/**
		 * aggregator
		 */
//		new Entry(new String[]{"-nostrict", "-force", "-threads", "1","-datasets", "datasets", "-metrics", "output", "lodanalysis.aggregator.Aggregator"});
//		new Entry(new String[]{"-nostrict", "-force", "-dataset", "encodingTest", "-metrics", "output", "lodanalysis.aggregator.Aggregator"});
//		java -jar -nostrict -threads 8 -datasets /path -output /output lodanalysis.aggregator.Aggregator
		//java -jar -path /datasests -output /output lodanalysis.metrics.CreateDescriptions
		/**
		 * create void descriptions
		 */
		new Entry(new String[]{"-nostrict", "-threads", "1","-datasets", "datasets", "-metrics", "output", "lodanalysis.metrics.CreateDescriptions"});
	}
}
