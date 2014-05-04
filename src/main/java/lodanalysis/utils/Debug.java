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
//		new Entry(new String[]{"-verbose", "-threads", "4", "-path", "Output", "lodanalysis.aggregator.Aggregator"});
//		new Entry(new String[]{"-threads", "4", "-path", "Output", "lodanalysis.aggregator.Aggregator", "lodanalysis.authority.CalcAuthority"});
//		new Entry(new String[]{"-path", "Output",  "lodanalysis.authority.CalcAuthority"});
//		new Entry(new String[]{"-path", "Output", "lodanalysis.aggregator.Aggregator"});
		new Entry(new String[]{"-force", "-dataset", "Output/f3109903df33520724131afd6b0a6a2e", "lodanalysis.aggregator.Aggregator"});
		
		/**
		 * Authority
		 */
//		new Entry(new String[]{"-dataset", "Output/c36ed6f8977c3aea234adb6d740a3740",  "lodanalysis.authority.CalcAuthority"});
		
		/**
		 * links
		 */
//		new Entry(new String[]{"-path", "Output",  "lodanalysis.links.CalcLinks"});
	}
}
