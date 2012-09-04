package tajo.cluster;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import tajo.TajoTestingUtility;
import tajo.engine.MasterInterfaceProtos.ServerStatusProto;
import tajo.engine.MasterInterfaceProtos.ServerStatusProto.Disk;
import tajo.engine.cluster.LeafServerTracker;
import tajo.engine.cluster.WorkerCommunicator;
import tajo.rpc.RemoteException;
import tajo.zookeeper.ZkClient;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestWorkerCommunicator {

  private static TajoTestingUtility cluster;
  private static ZkClient zkClient;
  private static LeafServerTracker tracker;
  private static WorkerCommunicator wc;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = new TajoTestingUtility();
    cluster.startMiniCluster(2);
    Thread.sleep(2000);

    zkClient = new ZkClient(cluster.getConfiguration());
    tracker = cluster.getMiniTajoCluster().getMaster().getTracker();

    wc = new WorkerCommunicator(zkClient, tracker);
    wc.start();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    wc.close();
    cluster.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    cluster.getMiniTajoCluster().startLeafServer();
    Thread.sleep(1000);
    assertEquals(wc.getProxyMap().size(), tracker.getMembers().size());

    cluster.getMiniTajoCluster().stopLeafServer(0, true);
    Thread.sleep(1500);
    assertEquals(wc.getProxyMap().size(), tracker.getMembers().size());

    List<String> servers = tracker.getMembers();
    for (String server : servers) {
      try {
        ServerStatusProto status = wc.getServerStatus(server).get();
        ServerStatusProto.System system = status.getSystem();

        assertNotNull(system.getAvailableProcessors());
        assertNotNull(system.getFreeMemory());
        assertNotNull(system.getMaxMemory());
        assertNotNull(system.getTotalMemory());

        List<Disk> diskStatuses = status.getDiskList();
        for (Disk diskStatus : diskStatuses) {
          assertNotNull(diskStatus.getAbsolutePath());
          assertNotNull(diskStatus.getTotalSpace());
          assertNotNull(diskStatus.getFreeSpace());
          assertNotNull(diskStatus.getUsableSpace());
        }

      } catch (RemoteException e) {
        System.out.println(e.getMessage());
      }
    }
  }

}