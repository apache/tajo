package nta.common.type;

import static org.junit.Assert.*;

import nta.common.exception.InvalidAddressException;

import org.junit.Test;

public class TestIPv4 {

	@Test
	public final void testSet() {
		IPv4 ip = null;
		IPv4 ip3;
		try {
			ip = new IPv4("255.255.255.255");
			ip3 = new IPv4("255.255.255.256");
		} catch (InvalidAddressException e) {
			System.out.println("InvalidAddressException is catched");
		}
		byte[] b = new byte[4];
		for (int i = 0; i < 4; i++) {
			b[i] = (byte)0xFF;
		}
		IPv4 ip2 = new IPv4(b);
		assertEquals(ip, ip2);
	}
	
	@Test
	public final void testEqual() throws InvalidAddressException {
		IPv4 ip1 = new IPv4("163.152.23.1");
		IPv4 ip2 = new IPv4("163.152.23.2");
		IPv4 ip3 = new IPv4("163.152.23.1");
		assertTrue(ip1.equals(ip3));
		assertFalse(ip1.equals(ip2));
		IPv4 ip4 = new IPv4("255.255.0.0");
		assertFalse(ip1.equals(ip4));
	}
	
	@Test
	public final void testAnd() throws InvalidAddressException {
		IPv4 ip1 = new IPv4("163.152.23.223");
		IPv4 ip2 = new IPv4("255.255.255.0");
		IPv4 ip3 = new IPv4("255.0.0.0");
		assertEquals(new IPv4("163.152.23.0"), ip1.and(ip2));
		assertFalse(ip1.and(ip2).equals(new IPv4("163.152.0.0")));
		assertTrue(ip1.and(ip3).equals(new IPv4("163.0.0.0")));
	}
	
	@Test
	public final void testMatchSubnet() throws InvalidAddressException {
		IPv4 ip1 = new IPv4("163.152.23.223");
		assertTrue(ip1.matchSubnet("163.152.23.0/1"));
		assertFalse(ip1.matchSubnet("163.152.23.0/28"));
	}
	
	@Test
	public final void testCompareTo() throws InvalidAddressException {
		IPv4 ip1 = new IPv4("163.152.23.1");
		IPv4 ip2 = new IPv4("163.152.23.2");
		IPv4 ip3 = new IPv4("177.234.123.12");
		IPv4 ip4 = new IPv4("177.234.123.12");
		assertTrue(ip1.compareTo(ip2) == -1);
		assertTrue(ip2.compareTo(ip1) == 1);
		assertTrue(ip3.compareTo(ip1) == 1);
		assertFalse(ip3.compareTo(ip2) != 1);
		assertTrue(ip4.compareTo(ip3) == 0);
	}
}
