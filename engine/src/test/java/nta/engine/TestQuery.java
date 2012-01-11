package nta.engine;

import static org.junit.Assert.*;

import java.io.IOException;

import nta.conf.NtaConf;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author jihoon
 *
 */
public class TestQuery {
	
	Configuration conf;
	NtaTestingUtility util;
	LocalNtaEngineCluster cluster;
	NtaEngine engine;
	
	@Before
	public void setup() throws Exception {
		// run cluster
		util = new NtaTestingUtility();
		util.startMiniCluster(10);
		conf = util.getConfiguration();
		cluster = new LocalNtaEngineCluster(conf, 10);
	}
	
	@After
	public void terminate() throws IOException {
		util.shutdownMiniCluster();
	}

	@Test
	public void testSelect() {
//		NtaEngineMaster master = cluster.getMaster();
//		while (true) {
//			
//		}
	}
}
