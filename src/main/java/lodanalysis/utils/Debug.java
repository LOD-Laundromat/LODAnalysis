package lodanalysis.utils;

import java.util.Date;

import lodanalysis.Entry;



/**
 * just a class for debugging stuff in eclipse. Then I wont need to switch to cmd ;)
 */
public class Debug {
	
	
	
	
	public static void main(String[] args) {
		
		//java -jar -path <path> lodanalysis.aggregator.Aggregator
//		new Entry(new String[]{"-path", "Output_old", "lodanalysis.authority.CalcAuthority"});
//		new Entry(new String[]{"-verbose", "-threads", "4", "-path", "Output", "lodanalysis.aggregator.Aggregator"});
		new Entry(new String[]{"-threads", "4", "-path", "Output", "lodanalysis.aggregator.Aggregator", "lodanalysis.authority.CalcAuthority"});
//		new Entry(new String[]{"-path", "Output", "lodanalysis.aggregator.Aggregator"});
//		new Entry(new String[]{"-force", "-dataset", "Output/7dd1ec8a2bb48b509e06af8e8d019ad1", "lodanalysis.aggregator.Aggregator"});
//		1e5a85569e906d2f0942dd61b098ade4
//		7dd1ec8a2bb48b509e06af8e8d019ad1
	}
}
