package nta.cluster;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import nta.engine.NtaTestingUtility;
import nta.engine.cluster.LeafServerStatusProtos.ServerStatusProto;
import nta.engine.cluster.LeafServerStatusProtos.ServerStatusProto.Disk;
import nta.engine.cluster.LeafServerTracker;
import nta.engine.cluster.WorkerCommunicator;
import nta.rpc.RemoteException;
import nta.zookeeper.ZkClient;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestWorkerCommunicator {

  private static NtaTestingUtility cluster;
  private static ZkClient zkClient;
  private static LeafServerTracker tracker;
  private static WorkerCommunicator wc;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = new NtaTestingUtility();
    cluster.startMiniCluster(2);
    Thread.sleep(2000);

    zkClient = new ZkClient(cluster.getConfiguration());
    tracker = cluster.getMiniNtaEngineCluster().getMaster().getTracker();

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
    cluster.getMiniNtaEngineCluster().startLeafServer();
    Thread.sleep(1000);
    assertEquals(wc.getProxyMap().size(), tracker.getMembers().size());

    cluster.getMiniNtaEngineCluster().stopLeafServer(0, true);
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