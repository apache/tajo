package nta.engine;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import nta.storage.Tuple;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNtaClient {
	NtaClient cli = null;
	
	@Before
	public void setUp() throws Exception {
//		cli = new NtaClient("localhost", 10000);
	}

	@After
	public void tearDown() throws Exception {
//		cli.close();
	}

	@Test
	public void testSubmit() throws IOException {
//		Tuple tuple = cli.executeQuery("").next();
//		assertNotNull(tuple);
//		assertEquals("hyunsik",tuple.getString(0));
//		assertEquals(1,tuple.getInt(1));
	}

}
