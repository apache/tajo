package nta.cluster;

import static org.junit.Assert.*;

import java.util.List;

import nta.engine.NtaTestingUtility;
import nta.engine.cluster.LeafServerStatusProtos.ServerStatusProto;
import nta.engine.cluster.LeafServerStatusProtos.ServerStatusProto.Disk;
import nta.engine.cluster.LeafServerTracker;
import nta.engine.cluster.WorkerCommunicator;
import nta.rpc.RemoteException;
import nta.zookeeper.ZkClient;

import org.junit.Test;

public class TestWorkerCommunicator {

  @Test
  public void test() throws Exception {
    NtaTestingUtility cluster = new NtaTestingUtility();
    cluster.startMiniCluster(2);
    Thread.sleep(1000);

    LeafServerTracker tracker = new LeafServerTracker(new ZkClient(
        cluster.getConfiguration()));

    WorkerCommunicator wc = new WorkerCommunicator(cluster.getConfiguration());
    wc.start();

    cluster.getMiniNtaEngineCluster().startLeafServer();
    Thread.sleep(1000);
    assertEquals(wc.getProxyMap().size(), tracker.getMembers().size());

    cluster.getMiniNtaEngineCluster().stopLeafServer(0, true);
    Thread.sleep(1500);
    assertEquals(wc.getProxyMap().size(), tracker.getMembers().size());

    List<String> servers = tracker.getMembers();
    for (String server : servers) {
      try {
        ServerStatusProto status; 
        status = wc.getServerStatus(server).get();

        assertNotNull(status.getSystem().getAvailableProcessors());
        assertNotNull(status.getSystem().getFreeMemory());
        assertNotNull(status.getSystem().getMaxMemory());
        assertNotNull(status.getSystem().getTotalMemory());

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

    wc.close();
    cluster.shutdownMiniCluster();
  }

}