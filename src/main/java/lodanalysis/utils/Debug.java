package lodanalysis.utils;

import java.util.Date;

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
//		new Entry(new String[]{"-nostrict", "-path", "Output",  "lodanalysis.authority.CalcAuthority"});
//		new Entry(new String[]{"-nostrict", "-path", "Output", "lodanalysis.aggregator.Aggregator"});
//		new Entry(new String[]{"-nostrict", "-force", "-dataset", "Output/5239ef6b90841c8d65efd311610458c4", "lodanalysis.aggregator.Aggregator"});
		
		/**
		 * Authority
		 */
//		new Entry(new String[]{"-nostrict", "-dataset", "Output/c36ed6f8977c3aea234adb6d740a3740",  "lodanalysis.authority.CalcAuthority"});
		
		/**
		 * links
		 */
		new Entry(new String[]{"-nostrict", "-path", "Output",  "lodanalysis.links.CalcLinks"});
	}
}
