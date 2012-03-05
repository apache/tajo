package nta.datum;

import static org.junit.Assert.*;

import java.util.Arrays;

import nta.datum.IPv4Datum;
import nta.datum.json.GsonCreator;

import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestIPv4Datum {

	@Before
	public void setUp() throws Exception {
	}
	
	@Test
	public final void testEquals() {
	  IPv4Datum ip1 = new IPv4Datum("192.168.0.1");
	  IPv4Datum ip2 = new IPv4Datum("192.168.0.1");
	  
	  assertEquals(ip1, ip2);
	  
	  IPv4Datum ip3 = new IPv4Datum(ip1.asByteArray());
	  assertEquals(ip1, ip3);
	  IPv4Datum ip4 = DatumFactory.createIPv4(ip1.asByteArray());
	  assertEquals(ip1, ip4);
	}

	@Test
	public final void testAsByteArray() {
		byte[] bytes = {(byte) 0xA3, (byte) 0x98, 0x17, (byte) 0xDE};
		IPv4Datum ip = new IPv4Datum(bytes);
		assertTrue(Arrays.equals(bytes, ip.asByteArray()));
	}

	@Test
	public final void testAsChars() {
		IPv4Datum ip = new IPv4Datum("163.152.23.222");
		assertEquals("163.152.23.222", ip.asChars());
	}
	
	@Test
  public final void testSize() {
    Datum d = DatumFactory.createIPv4("163.152.23.222");
    assertEquals(4, d.size());
  }
	
	@Test
	public final void testJson() {
		Datum d = DatumFactory.createIPv4("163.152.163.152");
		String json = d.toJSON();
		Datum fromJson = GsonCreator.getInstance().fromJson(json, Datum.class);
		assertTrue(d.equalsTo(fromJson).asBool());
	}
}
