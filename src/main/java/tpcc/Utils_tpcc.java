package tpcc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class Utils_tpcc {

	static AtomicBoolean atomicInitialized = new AtomicBoolean(false);
	static boolean waitForInit = true;
	static int scale = -10;

	// this function will be -dynamically- called from clojure at runtime
	public Utils_tpcc(int scale) {
		Utils_tpcc.scale = scale;
		Utils_tpcc.initialize(scale);
	}

	/*
	 * 
	 * INITIALIZATION CODE
	 * 
	 */
	public static void initialize(int scale) {
		if (atomicInitialized.compareAndSet(false, true)) {
			System.out.println("Utils_tpcc_" + scale + ": intializing data structures....");

			/*
			 * 
			 * THIS IS WHERE YOUR INITIALIZATION CODE GOES
			 * 
			 */

			waitForInit = false;
		} else {
			while (waitForInit)
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}
	}

	/*
	 * 
	 * AUXILIARY CODE
	 * 
	 */

	public static int get_w_id() {
		return ThreadLocalRandom.current().nextInt(1, scale + 1);
	}

	public static int get_d_id() {
		return ThreadLocalRandom.current().nextInt(1, 11);
	}

	public static int get_c_id() {
		return ThreadLocalRandom.current().nextInt(1, 3001);
	}

	public static int get_i_id() {
		return ThreadLocalRandom.current().nextInt(1, 100001);
	}

	public static int get_num_items() {
		return ThreadLocalRandom.current().nextInt(5, 16);
	}

	public static int[] get_item_ids(int num_items) {
		int[] itemIDs = new int[num_items];
		for (int i = 0; i < num_items; i++)
			itemIDs[i] = Utils_tpcc.get_i_id();
		return itemIDs;
	}

	public static int[] get_order_quantities(int num_items) {
		int[] orderQuantities = new int[num_items];
		for (int i = 0; i < num_items; i++)
			orderQuantities[i] = ThreadLocalRandom.current().nextInt(1, 11);
		return orderQuantities;
	}

	public static List<Object> get_sup_wh_and_o_all_local(int num_items, int terminal_w_id) {
		List<Object> result = new ArrayList<Object>();
		int allLocal = 1;
		int[] supplierWarehouseIDs = new int[num_items];
		for (int i = 0; i < num_items; i++)
			if (ThreadLocalRandom.current().nextInt(1, 101) > 1)
				supplierWarehouseIDs[i] = terminal_w_id;
			else {
				do
					supplierWarehouseIDs[i] = Utils_tpcc.get_w_id();
				while (supplierWarehouseIDs[i] == terminal_w_id && scale > 1);
				allLocal = 0;
			}

		result.add(supplierWarehouseIDs);
		result.add(allLocal);
		return result;
	}

}
