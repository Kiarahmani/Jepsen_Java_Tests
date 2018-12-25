import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class SeatsUtils {

	private static Map<Long, Integer> customerIdCount;

	public static long getNextCustomerId() {
		long depart_airport_id = ThreadLocalRandom.current().nextLong(2, 283);
		Integer customerId = customerIdCount.get(depart_airport_id);
		long id = ThreadLocalRandom.current().nextLong((customerId == null) ? 1 : (Math.max(customerId - 20, 1)));
		long composite_id = encode(new long[] { id, depart_airport_id }, COMPOSITE_BITS, COMPOSITE_POWS);
		System.out.println("composite_id: " + composite_id + "   --   id: " + id + "   --   depart_airport_id: "
				+ depart_airport_id + "   --   customerId: " + customerId);
		return composite_id;

	}

	public static long getNextAirlineId() {
		return nextLong(281474976710656L, 80501843339247631L);
	}

	public static long getNextFlightId() {
		return nextLong(89124701353L, 87750050550350462L);
	}

	public static long nextLong(long minimum, long maximum) {
		return ThreadLocalRandom.current().nextLong(minimum, maximum);
	}

	private static final int COMPOSITE_BITS[] = { 48, // ID
			16, // AIRPORT_ID
	};

	private static final long COMPOSITE_POWS[] = compositeBitsPreCompute(COMPOSITE_BITS);

	protected static final long[] compositeBitsPreCompute(int offset_bits[]) {
		long pows[] = new long[offset_bits.length];
		for (int i = 0; i < offset_bits.length; i++) {
			pows[i] = (long) (Math.pow(2, offset_bits[i]) - 1l);
		} // FOR
		return (pows);
	}

	public static long encode(long values[], int offset_bits[], long offset_pows[]) {
		assert (values.length == offset_bits.length);
		long id = 0;
		int offset = 0;
		for (int i = 0; i < values.length; i++) {
			id = (i == 0 ? values[i] : id | values[i] << offset);
			offset += offset_bits[i];
		} // FOR

		return (id);
	}

	public static void initializeCustomerMap() {
		customerIdCount = new HashMap<Long, Integer>();
		customerIdCount.put(1L, 53);
		customerIdCount.put(2L, 38);
		customerIdCount.put(3L, 531);
		customerIdCount.put(4L, 2);
		customerIdCount.put(5L, 6);
		customerIdCount.put(6L, 53);
		customerIdCount.put(7L, 12);
		customerIdCount.put(8L, 2);
		customerIdCount.put(9L, 11);
		customerIdCount.put(10L, 50);
		customerIdCount.put(11L, 61);
		customerIdCount.put(12L, 135);
		customerIdCount.put(13L, 101);
		customerIdCount.put(14L, 253);
		customerIdCount.put(15L, 111);
		customerIdCount.put(16L, 6558);
		customerIdCount.put(17L, 28);
		customerIdCount.put(18L, 709);
		customerIdCount.put(19L, 60);
		customerIdCount.put(20L, 20);
		customerIdCount.put(21L, 15);
		customerIdCount.put(22L, 365);
		customerIdCount.put(23L, 14);
		customerIdCount.put(24L, 47);
		customerIdCount.put(25L, 5);
		customerIdCount.put(26L, 266);
		customerIdCount.put(27L, 36);
		customerIdCount.put(28L, 38);
		customerIdCount.put(29L, 2);
		customerIdCount.put(30L, 10);
		customerIdCount.put(31L, 64);
		customerIdCount.put(32L, 874);
		customerIdCount.put(33L, 223);
		customerIdCount.put(34L, 1803);
		customerIdCount.put(35L, 2);
		customerIdCount.put(36L, 18);
		customerIdCount.put(37L, 29);
		customerIdCount.put(38L, 36);
		customerIdCount.put(39L, 2);
		customerIdCount.put(40L, 7);
		customerIdCount.put(41L, 121);
		customerIdCount.put(42L, 75);
		customerIdCount.put(43L, 383);
		customerIdCount.put(44L, 446);
		customerIdCount.put(45L, 1664);
		customerIdCount.put(46L, 32);
		customerIdCount.put(47L, 137);
		customerIdCount.put(48L, 119);
		customerIdCount.put(49L, 10);
		customerIdCount.put(50L, 11);
		customerIdCount.put(51L, 15);
		customerIdCount.put(52L, 54);
		customerIdCount.put(53L, 13);
		customerIdCount.put(54L, 191);
		customerIdCount.put(55L, 17);
		customerIdCount.put(56L, 83);
		customerIdCount.put(57L, 35);
		customerIdCount.put(58L, 810);
		customerIdCount.put(60L, 2108);
		customerIdCount.put(61L, 425);
		customerIdCount.put(62L, 32);
		customerIdCount.put(63L, 8);
		customerIdCount.put(64L, 12);
		customerIdCount.put(65L, 206);
		customerIdCount.put(66L, 22);
		customerIdCount.put(67L, 121);
		customerIdCount.put(68L, 57);
		customerIdCount.put(69L, 434);
		customerIdCount.put(70L, 16);
		customerIdCount.put(71L, 2);
		customerIdCount.put(72L, 35);
		customerIdCount.put(73L, 734);
		customerIdCount.put(74L, 200);
		customerIdCount.put(75L, 1);
		customerIdCount.put(76L, 1274);
		customerIdCount.put(77L, 3787);
		customerIdCount.put(78L, 4390);
		customerIdCount.put(79L, 30);
		customerIdCount.put(80L, 12);
		customerIdCount.put(81L, 55);
		customerIdCount.put(82L, 179);
		customerIdCount.put(83L, 1377);
		customerIdCount.put(84L, 15);
		customerIdCount.put(85L, 95);
		customerIdCount.put(86L, 61);
		customerIdCount.put(87L, 15);
		customerIdCount.put(88L, 2);
		customerIdCount.put(89L, 328);
		customerIdCount.put(90L, 66);
		customerIdCount.put(91L, 54);
		customerIdCount.put(92L, 9);
		customerIdCount.put(93L, 1818);
		customerIdCount.put(94L, 27);
		customerIdCount.put(95L, 85);
		customerIdCount.put(96L, 74);
		customerIdCount.put(97L, 166);
		customerIdCount.put(98L, 61);
		customerIdCount.put(99L, 23);
		customerIdCount.put(100L, 32);
		customerIdCount.put(101L, 1187);
		customerIdCount.put(102L, 2);
		customerIdCount.put(103L, 47);
		customerIdCount.put(104L, 73);
		customerIdCount.put(105L, 10);
		customerIdCount.put(106L, 41);
		customerIdCount.put(107L, 17);
		customerIdCount.put(108L, 232);
		customerIdCount.put(109L, 21);
		customerIdCount.put(110L, 2);
		customerIdCount.put(111L, 102);
		customerIdCount.put(112L, 33);
		customerIdCount.put(113L, 91);
		customerIdCount.put(114L, 73);
		customerIdCount.put(115L, 24);
		customerIdCount.put(116L, 187);
		customerIdCount.put(117L, 124);
		customerIdCount.put(118L, 135);
		customerIdCount.put(119L, 22);
		customerIdCount.put(120L, 6);
		customerIdCount.put(121L, 23);
		customerIdCount.put(122L, 2);
		customerIdCount.put(123L, 46);
		customerIdCount.put(124L, 18);
		customerIdCount.put(125L, 879);
		customerIdCount.put(126L, 858);
		customerIdCount.put(127L, 137);
		customerIdCount.put(128L, 84);
		customerIdCount.put(129L, 178);
		customerIdCount.put(130L, 1270);
		customerIdCount.put(131L, 2899);
		customerIdCount.put(132L, 164);
		customerIdCount.put(133L, 31);
		customerIdCount.put(134L, 49);
		customerIdCount.put(135L, 506);
		customerIdCount.put(136L, 5);
		customerIdCount.put(137L, 112);
		customerIdCount.put(138L, 132);
		customerIdCount.put(139L, 7);
		customerIdCount.put(140L, 75);
		customerIdCount.put(141L, 183);
		customerIdCount.put(142L, 477);
		customerIdCount.put(143L, 1532);
		customerIdCount.put(144L, 54);
		customerIdCount.put(145L, 191);
		customerIdCount.put(146L, 36);
		customerIdCount.put(147L, 22);
		customerIdCount.put(148L, 2447);
		customerIdCount.put(149L, 3224);
		customerIdCount.put(150L, 101);
		customerIdCount.put(151L, 14);
		customerIdCount.put(152L, 141);
		customerIdCount.put(153L, 103);
		customerIdCount.put(154L, 1644);
		customerIdCount.put(155L, 193);
		customerIdCount.put(156L, 177);
		customerIdCount.put(157L, 296);
		customerIdCount.put(158L, 10);
		customerIdCount.put(159L, 37);
		customerIdCount.put(160L, 39);
		customerIdCount.put(161L, 14);
		customerIdCount.put(162L, 5);
		customerIdCount.put(163L, 11);
		customerIdCount.put(164L, 2);
		customerIdCount.put(165L, 107);
		customerIdCount.put(166L, 12);
		customerIdCount.put(167L, 688);
		customerIdCount.put(168L, 2033);
		customerIdCount.put(169L, 83);
		customerIdCount.put(170L, 1390);
		customerIdCount.put(171L, 7);
		customerIdCount.put(172L, 862);
		customerIdCount.put(173L, 56);
		customerIdCount.put(174L, 54);
		customerIdCount.put(175L, 86);
		customerIdCount.put(176L, 2);
		customerIdCount.put(177L, 181);
		customerIdCount.put(178L, 1337);
		customerIdCount.put(179L, 768);
		customerIdCount.put(180L, 8);
		customerIdCount.put(181L, 24);
		customerIdCount.put(182L, 59);
		customerIdCount.put(183L, 19);
		customerIdCount.put(184L, 3);
		customerIdCount.put(185L, 114);
		customerIdCount.put(186L, 16);
		customerIdCount.put(187L, 29);
		customerIdCount.put(188L, 15);
		customerIdCount.put(189L, 86);
		customerIdCount.put(190L, 111);
		customerIdCount.put(191L, 38);
		customerIdCount.put(192L, 1630);
		customerIdCount.put(193L, 42);
		customerIdCount.put(194L, 35);
		customerIdCount.put(195L, 11);
		customerIdCount.put(196L, 730);
		customerIdCount.put(197L, 319);
		customerIdCount.put(198L, 334);
		customerIdCount.put(199L, 353);
		customerIdCount.put(200L, 10);
		customerIdCount.put(201L, 395);
		customerIdCount.put(202L, 4972);
		customerIdCount.put(203L, 220);
		customerIdCount.put(204L, 21);
		customerIdCount.put(205L, 9);
		customerIdCount.put(206L, 12);
		customerIdCount.put(207L, 509);
		customerIdCount.put(208L, 806);
		customerIdCount.put(209L, 61);
		customerIdCount.put(210L, 1435);
		customerIdCount.put(211L, 3193);
		customerIdCount.put(212L, 34);
		customerIdCount.put(213L, 2);
		customerIdCount.put(214L, 12);
		customerIdCount.put(215L, 508);
		customerIdCount.put(216L, 161);
		customerIdCount.put(217L, 43);
		customerIdCount.put(218L, 4);
		customerIdCount.put(219L, 4);
		customerIdCount.put(220L, 209);
		customerIdCount.put(221L, 311);
		customerIdCount.put(222L, 67);
		customerIdCount.put(223L, 83);
		customerIdCount.put(224L, 33);
		customerIdCount.put(225L, 45);
		customerIdCount.put(226L, 664);
		customerIdCount.put(227L, 239);
		customerIdCount.put(228L, 16);
		customerIdCount.put(229L, 374);
		customerIdCount.put(230L, 42);
		customerIdCount.put(231L, 163);
		customerIdCount.put(232L, 11);
		customerIdCount.put(233L, 10);
		customerIdCount.put(234L, 620);
		customerIdCount.put(235L, 9);
		customerIdCount.put(236L, 1243);
		customerIdCount.put(237L, 631);
		customerIdCount.put(238L, 129);
		customerIdCount.put(239L, 145);
		customerIdCount.put(240L, 24);
		customerIdCount.put(241L, 79);
		customerIdCount.put(242L, 9);
		customerIdCount.put(243L, 237);
		customerIdCount.put(244L, 1555);
		customerIdCount.put(245L, 2236);
		customerIdCount.put(246L, 118);
		customerIdCount.put(247L, 20);
		customerIdCount.put(248L, 82);
		customerIdCount.put(249L, 22);
		customerIdCount.put(250L, 649);
		customerIdCount.put(251L, 353);
		customerIdCount.put(252L, 2095);
		customerIdCount.put(253L, 678);
		customerIdCount.put(254L, 2);
		customerIdCount.put(255L, 660);
		customerIdCount.put(256L, 34);
		customerIdCount.put(257L, 2);
		customerIdCount.put(258L, 2);
		customerIdCount.put(259L, 120);
		customerIdCount.put(260L, 952);
		customerIdCount.put(261L, 68);
		customerIdCount.put(262L, 6);
		customerIdCount.put(263L, 38);
		customerIdCount.put(264L, 14);
		customerIdCount.put(265L, 122);
		customerIdCount.put(266L, 2);
		customerIdCount.put(267L, 86);
		customerIdCount.put(268L, 2);
		customerIdCount.put(269L, 1252);
		customerIdCount.put(270L, 65);
		customerIdCount.put(271L, 306);
		customerIdCount.put(272L, 377);
		customerIdCount.put(273L, 29);
		customerIdCount.put(274L, 23);
		customerIdCount.put(275L, 8);
		customerIdCount.put(276L, 159);
		customerIdCount.put(277L, 8);
		customerIdCount.put(278L, 123);
		customerIdCount.put(279L, 5);
		customerIdCount.put(280L, 170);
		customerIdCount.put(281L, 11);
		customerIdCount.put(282L, 68);
		customerIdCount.put(283L, 2);
		customerIdCount.put(284L, 684);
		customerIdCount.put(285L, 39);
		customerIdCount.put(286L, 8);

	}

}
