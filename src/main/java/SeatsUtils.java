import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class SeatsUtils {

	private static Map<Long, Integer> customerIdCount = new HashMap<Long, Integer>();

	public static long getNextCustomerId() {
		long depart_airport_id = ThreadLocalRandom.current().nextLong(1, 284);
		long id = ThreadLocalRandom.current().nextLong(1, customerIdCount.get(depart_airport_id));
		long composite_id = encode(new long[] { id, depart_airport_id }, COMPOSITE_BITS, COMPOSITE_POWS);
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
		customerIdCount.put(1L, 57);
		customerIdCount.put(3L, 567);
		customerIdCount.put(6L, 45);
		customerIdCount.put(9L, 16);
		customerIdCount.put(10L, 62);
		customerIdCount.put(11L, 64);
		customerIdCount.put(12L, 150);
		customerIdCount.put(13L, 114);
		customerIdCount.put(14L, 257);
		customerIdCount.put(15L, 113);
		customerIdCount.put(16L, 6485);
		customerIdCount.put(17L, 32);
		customerIdCount.put(18L, 648);
		customerIdCount.put(19L, 67);
		customerIdCount.put(20L, 26);
		customerIdCount.put(21L, 9);
		customerIdCount.put(22L, 377);
		customerIdCount.put(23L, 13);
		customerIdCount.put(24L, 55);
		customerIdCount.put(26L, 280);
		customerIdCount.put(27L, 48);
		customerIdCount.put(28L, 41);
		customerIdCount.put(31L, 55);
		customerIdCount.put(32L, 842);
		customerIdCount.put(33L, 262);
		customerIdCount.put(34L, 1816);
		customerIdCount.put(36L, 16);
		customerIdCount.put(37L, 7);
		customerIdCount.put(38L, 35);
		customerIdCount.put(39L, 5);
		customerIdCount.put(40L, 7);
		customerIdCount.put(41L, 126);
		customerIdCount.put(42L, 52);
		customerIdCount.put(43L, 366);
		customerIdCount.put(44L, 439);
		customerIdCount.put(45L, 1677);
		customerIdCount.put(46L, 65);
		customerIdCount.put(47L, 120);
		customerIdCount.put(48L, 138);
		customerIdCount.put(50L, 4);
		customerIdCount.put(52L, 63);
		customerIdCount.put(53L, 9);
		customerIdCount.put(54L, 192);
		customerIdCount.put(55L, 22);
		customerIdCount.put(56L, 91);
		customerIdCount.put(57L, 31);
		customerIdCount.put(58L, 802);
		customerIdCount.put(60L, 2091);
		customerIdCount.put(61L, 448);
		customerIdCount.put(62L, 33);
		customerIdCount.put(63L, 9);
		customerIdCount.put(65L, 215);
		customerIdCount.put(67L, 123);
		customerIdCount.put(68L, 56);
		customerIdCount.put(69L, 417);
		customerIdCount.put(71L, 1);
		customerIdCount.put(72L, 26);
		customerIdCount.put(73L, 735);
		customerIdCount.put(74L, 203);
		customerIdCount.put(76L, 1272);
		customerIdCount.put(77L, 3970);
		customerIdCount.put(78L, 4328);
		customerIdCount.put(79L, 17);
		customerIdCount.put(80L, 12);
		customerIdCount.put(81L, 58);
		customerIdCount.put(82L, 175);
		customerIdCount.put(83L, 1392);
		customerIdCount.put(84L, 14);
		customerIdCount.put(85L, 99);
		customerIdCount.put(86L, 62);
		customerIdCount.put(87L, 17);
		customerIdCount.put(89L, 353);
		customerIdCount.put(90L, 65);
		customerIdCount.put(91L, 55);
		customerIdCount.put(93L, 1836);
		customerIdCount.put(94L, 18);
		customerIdCount.put(95L, 63);
		customerIdCount.put(96L, 85);
		customerIdCount.put(97L, 179);
		customerIdCount.put(98L, 59);
		customerIdCount.put(99L, 13);
		customerIdCount.put(100L, 30);
		customerIdCount.put(101L, 1206);
		customerIdCount.put(103L, 54);
		customerIdCount.put(104L, 61);
		customerIdCount.put(105L, 6);
		customerIdCount.put(106L, 35);
		customerIdCount.put(107L, 23);
		customerIdCount.put(108L, 195);
		customerIdCount.put(110L, 3);
		customerIdCount.put(111L, 79);
		customerIdCount.put(112L, 35);
		customerIdCount.put(113L, 90);
		customerIdCount.put(114L, 53);
		customerIdCount.put(115L, 31);
		customerIdCount.put(116L, 182);
		customerIdCount.put(117L, 114);
		customerIdCount.put(118L, 138);
		customerIdCount.put(119L, 30);
		customerIdCount.put(120L, 10);
		customerIdCount.put(121L, 18);
		customerIdCount.put(122L, 1);
		customerIdCount.put(123L, 39);
		customerIdCount.put(124L, 20);
		customerIdCount.put(125L, 902);
		customerIdCount.put(126L, 879);
		customerIdCount.put(127L, 154);
		customerIdCount.put(128L, 92);
		customerIdCount.put(129L, 142);
		customerIdCount.put(130L, 1235);
		customerIdCount.put(131L, 2962);
		customerIdCount.put(132L, 158);
		customerIdCount.put(133L, 37);
		customerIdCount.put(134L, 55);
		customerIdCount.put(135L, 487);
		customerIdCount.put(137L, 124);
		customerIdCount.put(138L, 126);
		customerIdCount.put(139L, 11);
		customerIdCount.put(140L, 46);
		customerIdCount.put(141L, 178);
		customerIdCount.put(142L, 486);
		customerIdCount.put(143L, 1517);
		customerIdCount.put(144L, 56);
		customerIdCount.put(145L, 197);
		customerIdCount.put(146L, 34);
		customerIdCount.put(147L, 15);
		customerIdCount.put(148L, 2368);
		customerIdCount.put(149L, 3371);
		customerIdCount.put(150L, 123);
		customerIdCount.put(152L, 126);
		customerIdCount.put(153L, 74);
		customerIdCount.put(154L, 1673);
		customerIdCount.put(155L, 213);
		customerIdCount.put(156L, 183);
		customerIdCount.put(157L, 323);
		customerIdCount.put(158L, 19);
		customerIdCount.put(159L, 34);
		customerIdCount.put(163L, 5);
		customerIdCount.put(165L, 115);
		customerIdCount.put(167L, 766);
		customerIdCount.put(168L, 2104);
		customerIdCount.put(169L, 80);
		customerIdCount.put(170L, 1413);
		customerIdCount.put(172L, 904);
		customerIdCount.put(173L, 39);
		customerIdCount.put(174L, 62);
		customerIdCount.put(175L, 70);
		customerIdCount.put(177L, 192);
		customerIdCount.put(178L, 1230);
		customerIdCount.put(179L, 757);
		customerIdCount.put(180L, 5);
		customerIdCount.put(181L, 23);
		customerIdCount.put(182L, 88);
		customerIdCount.put(183L, 39);
		customerIdCount.put(184L, 2);
		customerIdCount.put(185L, 105);
		customerIdCount.put(186L, 21);
		customerIdCount.put(187L, 26);
		customerIdCount.put(188L, 8);
		customerIdCount.put(189L, 82);
		customerIdCount.put(190L, 111);
		customerIdCount.put(192L, 1600);
		customerIdCount.put(193L, 26);
		customerIdCount.put(194L, 53);
		customerIdCount.put(196L, 730);
		customerIdCount.put(197L, 320);
		customerIdCount.put(198L, 340);
		customerIdCount.put(199L, 323);
		customerIdCount.put(201L, 412);
		customerIdCount.put(202L, 4965);
		customerIdCount.put(203L, 211);
		customerIdCount.put(204L, 13);
		customerIdCount.put(205L, 14);
		customerIdCount.put(206L, 19);
		customerIdCount.put(207L, 536);
		customerIdCount.put(208L, 799);
		customerIdCount.put(209L, 78);
		customerIdCount.put(210L, 1395);
		customerIdCount.put(211L, 3064);
		customerIdCount.put(212L, 38);
		customerIdCount.put(213L, -1);
		customerIdCount.put(214L, 17);
		customerIdCount.put(215L, 565);
		customerIdCount.put(216L, 156);
		customerIdCount.put(217L, 66);
		customerIdCount.put(218L, 5);
		customerIdCount.put(220L, 217);
		customerIdCount.put(221L, 295);
		customerIdCount.put(222L, 65);
		customerIdCount.put(223L, 80);
		customerIdCount.put(224L, 27);
		customerIdCount.put(225L, 49);
		customerIdCount.put(226L, 712);
		customerIdCount.put(227L, 219);
		customerIdCount.put(228L, 14);
		customerIdCount.put(229L, 310);
		customerIdCount.put(230L, 35);
		customerIdCount.put(231L, 156);
		customerIdCount.put(232L, 11);
		customerIdCount.put(233L, 11);
		customerIdCount.put(234L, 595);
		customerIdCount.put(236L, 1243);
		customerIdCount.put(237L, 635);
		customerIdCount.put(238L, 152);
		customerIdCount.put(239L, 132);
		customerIdCount.put(240L, 34);
		customerIdCount.put(241L, 67);
		customerIdCount.put(242L, 5);
		customerIdCount.put(243L, 232);
		customerIdCount.put(244L, 1472);
		customerIdCount.put(245L, 2133);
		customerIdCount.put(246L, 119);
		customerIdCount.put(247L, 23);
		customerIdCount.put(248L, 63);
		customerIdCount.put(250L, 640);
		customerIdCount.put(251L, 365);
		customerIdCount.put(252L, 2069);
		customerIdCount.put(253L, 751);
		customerIdCount.put(254L, 15);
		customerIdCount.put(255L, 699);
		customerIdCount.put(256L, 24);
		customerIdCount.put(259L, 103);
		customerIdCount.put(260L, 972);
		customerIdCount.put(261L, 56);
		customerIdCount.put(262L, 6);
		customerIdCount.put(263L, 40);
		customerIdCount.put(264L, 12);
		customerIdCount.put(265L, 133);
		customerIdCount.put(266L, 10);
		customerIdCount.put(267L, 82);
		customerIdCount.put(268L, -2);
		customerIdCount.put(269L, 1231);
		customerIdCount.put(270L, 65);
		customerIdCount.put(271L, 266);
		customerIdCount.put(272L, 393);
		customerIdCount.put(273L, 27);
		customerIdCount.put(274L, 22);
		customerIdCount.put(275L, 8);
		customerIdCount.put(276L, 179);
		customerIdCount.put(277L, 19);
		customerIdCount.put(278L, 113);
		customerIdCount.put(279L, 13);
		customerIdCount.put(280L, 196);
		customerIdCount.put(281L, 6);
		customerIdCount.put(282L, 56);
		customerIdCount.put(284L, 652);
		customerIdCount.put(285L, 23);

	}

}
