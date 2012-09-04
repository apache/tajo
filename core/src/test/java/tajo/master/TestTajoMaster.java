package tajo.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.NConstants;
import tajo.TajoTestingUtility;
import tajo.conf.TajoConf;
import tajo.engine.cluster.ServerNodeTracker;
import tajo.zookeeper.ZkClient;
import tajo.zookeeper.ZkUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestTajoMaster {
  private Log LOG = LogFactory.getLog(TestTajoMaster.class);
	private TajoTestingUtility util;
	private TajoConf conf;
	
	private final int numLeafs = 3;

	@Before
	public void setUp() throws Exception {
		util = new TajoTestingUtility();
    util.startMiniCluster(numLeafs);
		conf = util.getConfiguration();
	}

	@After
	public void tearDown() throws Exception {
		util.shutdownMiniCluster();
	}

	@Test
  public void testBecomeMaster() throws Exception {
    ZkClient zkClient = new ZkClient(conf);
    ServerNodeTracker tracker = new ServerNodeTracker(zkClient,
        NConstants.ZNODE_BASE);
    LOG.info("Waiting for the participation of leafservers");
    tracker.blockUntilAvailable(3000);
    assertNotNull(zkClient.exists(NConstants.ZNODE_BASE));
    assertNotNull(zkClient.exists(NConstants.ZNODE_MASTER));
    assertNotNull(zkClient.exists(NConstants.ZNODE_CLIENTSERVICE));
    assertNotNull(zkClient.exists(NConstants.ZNODE_LEAFSERVERS));
    assertNotNull(zkClient.exists(NConstants.ZNODE_QUERIES));

    byte[] data = ZkUtil.getDataAndWatch(zkClient, NConstants.ZNODE_MASTER);

    TajoMaster master = util.getMiniTajoCluster().getMaster();
    assertEquals(master.getMasterServerName(), new String(data));
    
    data = ZkUtil.getDataAndWatch(zkClient, NConstants.ZNODE_CLIENTSERVICE);
    assertEquals(master.getClientServiceServerName(), new String(data));
    zkClient.close();
  }
}
