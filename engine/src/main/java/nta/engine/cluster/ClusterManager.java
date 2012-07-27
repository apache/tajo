package nta.engine.cluster;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Maps;
import nta.catalog.CatalogClient;
import nta.catalog.FragmentServInfo;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.TableDescProto;
import nta.engine.MasterInterfaceProtos.ServerStatusProto;
import nta.engine.MasterInterfaceProtos.ServerStatusProto.Disk;
import nta.engine.QueryUnitId;
import nta.engine.exception.UnknownWorkerException;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.rpc.RemoteException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ClusterManager {
  private final int FRAG_DIST_THRESHOLD = 3;
  private final Log LOG = LogFactory.getLog(ClusterManager.class);

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

  private class FragmentAssignInfo
  implements Comparable<FragmentAssignInfo> {
    String serverName;
    int fragmentNum;
    
    public FragmentAssignInfo(String serverName, int fragmentNum) {
      this.serverName = serverName;
      this.fragmentNum = fragmentNum;
    }
    
    public FragmentAssignInfo updateFragmentNum() {
      this.fragmentNum++;
      return this;
    }
    
    @Override
    public int compareTo(FragmentAssignInfo o) {
      return this.fragmentNum - o.fragmentNum;
    }
  }
  
  private Configuration conf;
  private final WorkerCommunicator wc;
  private final CatalogClient catalog;
  private final LeafServerTracker tracker;

  private int clusterSize;
  private Map<String, List<String>> DNSNameToHostsMap = 
      new HashMap<String, List<String>>();

  private Map<Fragment, FragmentServingInfo> servingInfoMap;

  public ClusterManager(WorkerCommunicator wc, Configuration conf,
      LeafServerTracker tracker) throws IOException {
    this.wc = wc;
    this.conf = conf;
    this.catalog = new CatalogClient(this.conf);
    this.tracker = tracker;
    servingInfoMap = Maps.newHashMap();
    this.clusterSize = 0;
  }

  public WorkerInfo getWorkerInfo(String workerName) throws RemoteException,
      InterruptedException, ExecutionException, UnknownWorkerException {
    ServerStatusProto status = wc.getServerStatus(workerName).get();
    ServerStatusProto.System system = status.getSystem();
    WorkerInfo info = new WorkerInfo();
    info.availableProcessors = system.getAvailableProcessors();
    info.freeMemory = system.getFreeMemory();
    info.totalMemory = system.getTotalMemory();

    for (Disk diskStatus : status.getDiskList()) {
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

  public void updateOnlineWorker() {
    String DNSName;
    List<String> workers;
    DNSNameToHostsMap.clear();
    clusterSize = 0;
    for (String worker : tracker.getMembers()) {
      DNSName = worker.split(":")[0];
      if (DNSNameToHostsMap.containsKey(DNSName)) {
        workers = DNSNameToHostsMap.get(DNSName);
      } else {
        workers = new ArrayList<String>();
      }
      workers.add(worker);
      clusterSize++;
      DNSNameToHostsMap.put(DNSName, workers);
    }
  }
  
  public Map<String, List<String>> getOnlineWorkers() {
    return this.DNSNameToHostsMap;
  }

  public void updateFragmentServingInfo2(String table) throws IOException {
    TableDescProto td = (TableDescProto) catalog.getTableDesc(table).getProto();
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(td.getPath());

    Map<String, StoredBlockInfo> storedBlockInfoMap =
        ClusterManagerUtils.makeStoredBlockInfoForHosts(fs, path);
    Map<Fragment, FragmentServingInfo> assignMap =
        ClusterManagerUtils.assignFragments(td, storedBlockInfoMap.values());
    this.servingInfoMap.putAll(assignMap);
  }

  public Map<Fragment, FragmentServingInfo> getServingInfoMap() {
    return this.servingInfoMap;
  }
  
  private String getRandomWorkerNameOfHost(String host) {
    Random rand = new Random(System.currentTimeMillis());
    List<String> workers = DNSNameToHostsMap.get(host);
    return workers.get(rand.nextInt(workers.size()));
  }

  private List<FragmentServInfo> getFragmentLocInfo(TableDescProto desc)
      throws IOException {
    int fileIdx, blockIdx;
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(desc.getPath());
    
    FileStatus[] files = fs.listStatus(new Path(path + "/data"));
    if (files == null || files.length == 0) {
      throw new FileNotFoundException(path.toString() + "/data");
    }
    BlockLocation[] blocks;
    String[] hosts;
    List<FragmentServInfo> fragmentInfoList = 
        new ArrayList<FragmentServInfo>();
    
    for (fileIdx = 0; fileIdx < files.length; fileIdx++) {
      blocks = fs.getFileBlockLocations(files[fileIdx], 0,
          files[fileIdx].getLen());
      for (blockIdx = 0; blockIdx < blocks.length; blockIdx++) {
        hosts = blocks[blockIdx].getHosts();
        // TODO: select the proper serving node for block
        Fragment fragment = new Fragment(desc.getId(),
            files[fileIdx].getPath(), new TableMetaImpl(desc.getMeta()),
            blocks[blockIdx].getOffset(), blocks[blockIdx].getLength());
        fragmentInfoList.add(new FragmentServInfo(hosts[0], -1, fragment));
      }
    }
    return fragmentInfoList;
  }
  
  /**
   * Select a random worker
   * 
   * @return
   * @throws Exception
   */
  public String getRandomHost() 
      throws Exception {
    Random rand = new Random(System.currentTimeMillis());
    int n = rand.nextInt(DNSNameToHostsMap.size());
    Iterator<List<String>> it = DNSNameToHostsMap.values().iterator();
    for (int i = 0; i < n-1; i++) {
      it.next();
    }
    List<String> hosts = it.next();
    return hosts.get(rand.nextInt(hosts.size()));
  }
}