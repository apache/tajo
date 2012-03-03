package nta.engine;

import static org.junit.Assert.*;

import nta.engine.planner.physical.SleepExec;
import nta.engine.utils.JVMClusterUtil.LeafServerThread;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNtaTestingUtility {
  NtaTestingUtility util;
  int num = 4;

  @Before
  public void setUp() throws Exception {
    util = new NtaTestingUtility();
    util.startMiniCluster(num);
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public final void test() throws InterruptedException {
    Thread.sleep(2000);
    
    SleepExec [] execs = new SleepExec[num];
    for (int i = 0; i < 4; i++) {
      execs[i] = new SleepExec("node " + i+ " says, zz... zz... zz...");
    }
    
    int i = 0;
    for (LeafServerThread leaf 
        : util.getMiniNtaEngineCluster().getLeafServerThreads()) {
      leaf.getLeafServer().requestTestQuery(execs[i]);
      i++;
    }
    
    Thread.sleep(1000);
    LeafServer leaf0 = util.getMiniNtaEngineCluster().getLeafServer(0);
    leaf0.shutdown("Aborted!");

    Thread.sleep(1000);
    LeafServer leaf1 = util.getMiniNtaEngineCluster().getLeafServer(1);
    leaf1.shutdown("Aborted!");
    
    Thread.sleep(1000);
    LeafServer leaf2 = util.getMiniNtaEngineCluster().getLeafServer(2);
    leaf2.shutdown("Aborted!");
    
    Thread.sleep(1000);
    LeafServer leaf3 = util.getMiniNtaEngineCluster().getLeafServer(3);
    leaf3.shutdown("Aborted!");
    
    assertFalse(leaf0.isAlive());
    assertFalse(leaf1.isAlive());
    assertFalse(leaf2.isAlive());
    assertFalse(leaf3.isAlive());
  }
}
