package tajo.cluster;

import org.junit.Test;
import tajo.master.cluster.ServerName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestServerName {

	@Test
	public void testServerNameStringInt() {
		ServerName server = new ServerName("ex1.com",50030);
		assertEquals("ex1.com", server.getHostname());
		assertEquals(50030, server.getPort());
	}

	@Test
	public void testServerNameString() {
		ServerName server = new ServerName("ex1.com:50030");
		assertEquals("ex1.com", server.getHostname());
		assertEquals(50030, server.getPort());
	}

	@Test
	public void testParseHostname() {
		assertEquals("ex1.com",ServerName.parseHostname("ex1.com:50030"));
	}

	@Test
	public void testParsePort() {
		assertEquals(50030,ServerName.parsePort("ex1.com:50030"));
	}

	@Test
	public void testToString() {
		ServerName server = new ServerName("ex1.com",50030);
		assertEquals("ex1.com:50030", server.toString());
	}

	@Test
	public void testGetServerName() {
		ServerName server = new ServerName("ex1.com",50030);
		assertEquals("ex1.com:50030", server.getServerName());
	}

	@Test
	public void testGetHostname() {
		ServerName server = new ServerName("ex1.com",50030);
		assertEquals("ex1.com", server.getHostname());
	}

	@Test
	public void testGetPort() {
		ServerName server = new ServerName("ex1.com",50030);
		assertEquals(50030, server.getPort());
	}

	@Test
	public void testGetServerNameStringInt() {
		assertEquals("ex2.com:50030",ServerName.getServerName("ex2.com", 50030));
	}

	@Test
	public void testCompareTo() {
		ServerName s1 = new ServerName("ex1.com:50030");
		ServerName s2 = new ServerName("ex1.com:60030");
		
		assertTrue(s1.compareTo(s2) < 0);
		assertTrue(s2.compareTo(s1) > 0);
		
		ServerName s3 = new ServerName("ex1.com:50030");
		assertTrue(s1.compareTo(s3) == 0);
		
		ServerName s4 = new ServerName("ex2.com:50030");
		assertTrue(s1.compareTo(s4) < 0);
		assertTrue(s4.compareTo(s1) > 0);
	}

  @Test (expected = IllegalArgumentException.class)
  public void testException() {
    new ServerName("ex1.com");
  }
}
