package nta.engine.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import nta.catalog.CatalogClient;
import nta.catalog.HostInfo;
import nta.catalog.TableDesc;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.TableDescProto;
import nta.engine.QueryUnitId;
import nta.engine.cluster.LeafServerStatusProtos.ServerStatusProto;
import nta.engine.cluster.LeafServerStatusProtos.ServerStatusProto.Disk;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.rpc.RemoteException;
import nta.zookeeper.ZkClient;

public class ClusterManager {

  public class WorkerInfo {
    public int availableProcessors;
    public long freeMemory;
    public long totalMemory;

    public List<DiskInfo> disks = new ArrayList<DiskInfo>();
  }

  public class DiskInfo {
    public long freeSpace;
    public long totalSpace;
  }

  private WorkerCommunicator wc;

  private Map<String, List<HostInfo>> tabletServingInfo = new HashMap<String, List<HostInfo>>();
  private Map<String, List<Fragment>> fragLoc = new HashMap<String, List<Fragment>>();
  private Map<Fragment, List<String>> workerLoc = new HashMap<Fragment, List<String>>();
  private Configuration conf;
  private final CatalogClient catalog;
  private LeafServerTracker tracker;

  public ClusterManager(WorkerCommunicator wc, Configuration conf,
      LeafServerTracker tracker) throws IOException {
    this.wc = wc;
    this.conf = conf;
    this.catalog = new CatalogClient(this.conf);
    this.tracker = tracker;
  }

  public WorkerInfo getWorkerInfo(String workerName) throws RemoteException,
      InterruptedException, ExecutionException {
    ServerStatusProto status = wc.getServerStatus(workerName).get();
    ServerStatusProto.System system = status.getSystem();
    WorkerInfo info = new WorkerInfo();
    info.availableProcessors = system.getAvailableProcessors();
    info.freeMemory = system.getFreeMemory();
    info.totalMemory = system.getTotalMemory();

    List<Disk> diskStatuses = status.getDiskList();

    for (Disk diskStatus : diskStatuses) {
      DiskInfo diskInfo = new DiskInfo();
      diskInfo.freeSpace = diskStatus.getFreeSpace();
      diskInfo.totalSpace = diskStatus.getTotalSpace();
      info.disks.add(diskInfo);
    }
    return info;
  }

  public List<QueryUnitId> getProcessingQuery(String workerName) {
    return null;
  }

  public List<String> getOnlineWorker(ZkClient zkClient) throws Exception {
    return tracker.getMembers();
  }

  public List<String> getWorkerbyFrag(Fragment fragment) throws Exception {
    return workerLoc.get(fragment);
  }

  public List<Fragment> getFragbyWorker(String workerName) throws Exception {
    return fragLoc.get(workerName);
  }

  public void updateAllTabletServingInfo(List<String> onlineServers)
      throws Exception {

    tabletServingInfo.clear();
    fragLoc.clear();
    workerLoc.clear();
    Iterator<String> it = catalog.getAllTableNames().iterator();
    List<HostInfo> locInfos;
    List<HostInfo> servInfos;
    int index = 0;
    StringTokenizer tokenizer;
    while (it.hasNext()) {
      TableDescProto td = (TableDescProto) catalog.getTableDesc(it.next())
          .getProto();
      locInfos = getTabletLocInfo(td);
      servInfos = new ArrayList<HostInfo>();

      // TODO: select the proper online server
      for (HostInfo servInfo : locInfos) {
        // round robin
        if (index == onlineServers.size()) {
          index = 0;
        }
        String workerName = onlineServers.get(index++);
        tokenizer = new StringTokenizer(workerName, ":");
        servInfo.setHost(tokenizer.nextToken(),
            Integer.valueOf(tokenizer.nextToken()));
        servInfos.add(servInfo);

        setFragLoc(servInfo, workerName);

      }
      tabletServingInfo.put(td.getId(), servInfos);
    }
  }

  private void setFragLoc(HostInfo servInfo, String workerName) {
    List<Fragment> tempFrag;
    if (fragLoc.containsKey(workerName)) {
      tempFrag = fragLoc.get(workerName);
    } else {
      tempFrag = new ArrayList<Fragment>();
    }
    tempFrag.add(servInfo.getFragment());
    fragLoc.put(workerName, tempFrag);
  }

  private void setWorkerLoc(Fragment fragment, String[] workerNames) {
    List<String> tempWorker;

    tempWorker = new ArrayList<String>();

    for (String workerName : workerNames) {
      tempWorker.add(workerName);
    }
    workerLoc.put(fragment, tempWorker);
  }

  private List<HostInfo> getTabletLocInfo(TableDescProto desc)
      throws IOException {
    int fileIdx, blockIdx;
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(desc.getPath());

    FileStatus[] files = fs.listStatus(new Path(path + "/data"));
    BlockLocation[] blocks;
    String[] hosts;
    List<HostInfo> tabletInfoList = new ArrayList<HostInfo>();

    int i = 0;
    for (fileIdx = 0; fileIdx < files.length; fileIdx++) {
      blocks = fs.getFileBlockLocations(files[fileIdx], 0,
          files[fileIdx].getLen());
      for (blockIdx = 0; blockIdx < blocks.length; blockIdx++) {
        hosts = blocks[blockIdx].getHosts();

        // TODO: select the proper serving node for block
        Fragment fragment = new Fragment(desc.getId() + "_" + i,
            files[fileIdx].getPath(), new TableMetaImpl(desc.getMeta()),
            blocks[blockIdx].getOffset(), blocks[blockIdx].getLength());
        tabletInfoList.add(new HostInfo(hosts[0], -1, fragment));
        i++;

        setWorkerLoc(fragment, blocks[blockIdx].getNames());
      }
    }
    return tabletInfoList;
  }

  public TableDesc getTableDesc(String table) {
    return catalog.getTableDesc(table);
  }
}