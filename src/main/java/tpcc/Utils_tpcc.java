package tpcc;

public class Utils_tpcc {

	// this function will be -dynamically- called from clojure at runtime
	public Utils_tpcc() {
		Utils_tpcc.initialize();
	}

	
	/*
	 * 
	 * 
	 */
	public static void initialize() {
		System.out.println("Utils_tpcc: intializing data structures....");
	}
}
