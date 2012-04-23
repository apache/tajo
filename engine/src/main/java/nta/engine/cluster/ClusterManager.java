package nta.engine.cluster;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import nta.catalog.CatalogClient;
import nta.catalog.FragmentServInfo;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.TableDescProto;
import nta.engine.MasterInterfaceProtos.ServerStatusProto;
import nta.engine.MasterInterfaceProtos.ServerStatusProto.Disk;
import nta.engine.QueryUnitId;
import nta.engine.exception.UnknownWorkerException;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.logical.ScanNode;
import nta.rpc.RemoteException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ClusterManager {
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
  
  private Configuration conf;
  private final WorkerCommunicator wc;
  private final CatalogClient catalog;
  private final LeafServerTracker tracker;

  private Map<String, HashSet<Fragment>> fragLoc = 
      new HashMap<String, HashSet<Fragment>>();
  private Map<Fragment, String> workerLoc = 
      new HashMap<Fragment, String>();

  public ClusterManager(WorkerCommunicator wc, Configuration conf,
      LeafServerTracker tracker) throws IOException {
    this.wc = wc;
    this.conf = conf;
    this.catalog = new CatalogClient(this.conf);
    this.tracker = tracker;
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

  public List<String> getOnlineWorker() {
    return tracker.getMembers();
  }

  public String getWorkerbyFrag(Fragment fragment) throws Exception {
    return workerLoc.get(fragment);
  }

  public Set<Fragment> getFragbyWorker(String workerName) throws Exception {
    return fragLoc.get(workerName);
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

  public void updateAllFragmentServingInfo(List<String> onlineServers) throws IOException {
    fragLoc.clear();
    workerLoc.clear();
    
    Map<String, PriorityQueue<FragmentAssignInfo>> servers = 
        new HashMap<String, PriorityQueue<FragmentAssignInfo>>();
    PriorityQueue<FragmentAssignInfo> assignInfos;
    String host;
    for (String serverName : onlineServers) {
      host = serverName.split(":")[0];
      if (servers.containsKey(host)) {
        assignInfos = servers.get(host);
      } else {
        assignInfos = new PriorityQueue<FragmentAssignInfo>();
      }
      assignInfos.add(new FragmentAssignInfo(serverName, 0));
      servers.put(host, assignInfos);
    }
    
    Iterator<String> it = catalog.getAllTableNames().iterator();
    List<FragmentServInfo> locInfos;
    FragmentAssignInfo assignInfo = null;
    while (it.hasNext()) {
      TableDescProto td = (TableDescProto) catalog.getTableDesc(it.next())
          .getProto();
      locInfos = getFragmentLocInfo(td);
      
      // TODO: select the proper online server
      for (FragmentServInfo locInfo : locInfos) {
        assignInfos = servers.get(locInfo.getHostName());
        if (assignInfos == null) {
          assignInfos = servers.values().iterator().next();
        } 
        assignInfo = assignInfos.poll();
        locInfo.setHost(assignInfo.serverName);
        assignInfos.add(assignInfo.updateFragmentNum());
        updateFragLoc(locInfo, assignInfo.serverName);
      }
    }
  }

  private void updateFragLoc(FragmentServInfo servInfo, String workerName) {
    HashSet<Fragment> tempFrag;
    if (fragLoc.containsKey(workerName)) {
      tempFrag = fragLoc.get(workerName);
    } else {
      tempFrag = new HashSet<Fragment>();
    }
    tempFrag.add(servInfo.getFragment());
    fragLoc.put(workerName, tempFrag);
    
    workerLoc.put(servInfo.getFragment(), workerName);
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
    Random rand = new Random();
    List<String> serverNames = this.getOnlineWorker();
    return serverNames.get(rand.nextInt(serverNames.size()));
  }
  
  /**
   * Select a worker which servers most fragments of the query unit
   * 
   * @param unit
   * @return
   * @throws Exception
   */
  public String getProperHost(QueryUnit unit) throws Exception {
    // unit의 fragment 중 가장 많은 fragment를 담당하는 worker에게 할당

    Map<String, Integer> map = new HashMap<String, Integer>();
    String serverName;
    // fragment를 담당하는 worker를 담당하는 fragment의 수에 따라 정렬
    for (ScanNode scan : unit.getScanNodes()) {
      for (Fragment frag : unit.getFragments(scan.getTableId())) {
        serverName = this.getWorkerbyFrag(frag);
        if (map.containsKey(serverName)) {
          map.put(serverName, map.get(serverName)+1);
        } else {
          map.put(serverName, 1);
        }
      }
    }
    PriorityQueue<FragmentAssignInfo> pq = 
        new PriorityQueue<ClusterManager.FragmentAssignInfo>();
    Iterator<Entry<String,Integer>> it = map.entrySet().iterator();
    Entry<String,Integer> e;
    while (it.hasNext()) {
      e = it.next();
      pq.add(new FragmentAssignInfo(e.getKey(), e.getValue()));
    }
    
    List<String> serverNames = this.getOnlineWorker();
    // 가장 많은 fragment를 담당하는 worker부터 online worker에 있는지 확인
    for (FragmentAssignInfo assignInfo : pq) {
      if (serverNames.contains(assignInfo.serverName)) {
        return assignInfo.serverName;
      }
    }
    return null;
  }
}