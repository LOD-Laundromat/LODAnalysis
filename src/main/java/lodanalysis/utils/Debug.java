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
//		new Entry(new String[]{"-nostrict", "-verbose", "-threads", "4","-force", "-path", "Output", "lodanalysis.aggregator.Aggregator"});
//		new Entry(new String[]{"-nostrict", "-threads", "4", "-path", "Output", "lodanalysis.aggregator.Aggregator", "lodanalysis.authority.CalcAuthority"});
//		new Entry(new String[]{"-nostrict", "-force", "-threads", "1","-path", "datasets", "-output", "output", "lodanalysis.aggregator.Aggregator"});
//		new Entry(new String[]{"-nostrict", "-force", "-dataset", "Output/5239ef6b90841c8d65efd311610458c4", "lodanalysis.aggregator.Aggregator"});
		
		/**
		 * create void descriptions
		 */
		new Entry(new String[]{"-nostrict", "-threads", "1","-path", "datasets", "-output", "output", "lodanalysis.description.CreateDescriptions"});
	}
}
