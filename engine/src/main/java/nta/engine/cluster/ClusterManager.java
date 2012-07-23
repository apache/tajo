package nta.engine.cluster;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
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
import org.apache.hadoop.thirdparty.guava.common.collect.Maps;

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

  private Map<String, List<String>> DNSNameToHostsMap = 
      new HashMap<String, List<String>>();
  private Map<String, HashSet<Fragment>> fragLoc = 
      new HashMap<String, HashSet<Fragment>>();
  private Map<Fragment, String> workerLoc = 
      new HashMap<Fragment, String>();
  private PriorityQueue<FragmentAssignInfo> servingInfos =
      new PriorityQueue<FragmentAssignInfo>();

  private Map<Fragment, FragmentServingInfo> servingInfoMap;

  public ClusterManager(WorkerCommunicator wc, Configuration conf,
      LeafServerTracker tracker) throws IOException {
    this.wc = wc;
    this.conf = conf;
    this.catalog = new CatalogClient(this.conf);
    this.tracker = tracker;
    servingInfoMap = Maps.newHashMap();
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
    for (String worker : tracker.getMembers()) {
      DNSName = worker.split(":")[0];
      if (DNSNameToHostsMap.containsKey(DNSName)) {
        workers = DNSNameToHostsMap.get(DNSName);
      } else {
        workers = new ArrayList<String>();
      }
      workers.add(worker);
      DNSNameToHostsMap.put(DNSName, workers);
      servingInfos.add(new FragmentAssignInfo(worker, 0));
    }
  }
  
  public Map<String, List<String>> getOnlineWorkers() {
    return this.DNSNameToHostsMap;
  }

  public String getWorkerbyFrag(Fragment fragment) throws Exception {
    return workerLoc.get(fragment);
  }

  public Set<Fragment> getFragbyWorker(String workerName) throws Exception {
    return fragLoc.get(workerName);
  }
  
  public void updateFragmentServingInfo(String table) 
      throws IOException {
    int fileIdx, blockIdx;
    TableDescProto td = (TableDescProto) catalog.getTableDesc(table).getProto();
    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(td.getPath());
    BlockLocation[] blocks;

    // get files of the table
    FileStatus[] files = fs.listStatus(new Path(path + "/data"));
    if (files == null || files.length == 0) {
      throw new FileNotFoundException(path.toString() + "/data");
    }

    for (fileIdx = 0; fileIdx < files.length; fileIdx++) {
      // get blocks of each file
      blocks = fs.getFileBlockLocations(files[fileIdx], 0,
          files[fileIdx].getLen());
      for (blockIdx = 0; blockIdx < blocks.length; blockIdx++) {
        // make fragments
        Fragment f = new Fragment(td.getId(),
            files[fileIdx].getPath(), new TableMetaImpl(td.getMeta()),
            blocks[blockIdx].getOffset(), blocks[blockIdx].getLength());
        // get hosts of each block and assign fragment to the proper host
        String host = getServHostForFragment(f, blocks[blockIdx].getHosts());
        String worker = getRandomWorkerNameOfHost(host);
        updateFragLoc(new FragmentServInfo(worker, f), worker);
      }
    }
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
  
  private String getServHostForFragment(Fragment f, String[] hosts) {
    String host = null;
    int i;
    int assignNum;
    if(fragLoc.containsKey(hosts[0])) {
      assignNum = fragLoc.get(hosts[0]).size();
    } else {
      assignNum = 0;
    }
    for (i = 0; i < hosts.length; i++) {
      if (fragLoc.containsKey(hosts[i])) {
        if (assignNum > FRAG_DIST_THRESHOLD + fragLoc.get(hosts[i]).size()) {
          break;
        }
      }
    }
    
    if (i == hosts.length
        && DNSNameToHostsMap.containsKey(hosts[0].split(":")[0])) {
      host = hosts[0];
    } else {
      if (i < hosts.length) {
        if (fragLoc.containsKey(hosts[i])) {
          assignNum = fragLoc.get(hosts[i]).size();
        } else {
          assignNum = 0;
        }
        if ((servingInfos.peek().fragmentNum + FRAG_DIST_THRESHOLD) >= assignNum
            && DNSNameToHostsMap.containsKey(hosts[i].split(":")[0])) {
          host = hosts[i];
        } else {
          host = servingInfos.peek().serverName;
        }
      } else {
        host = servingInfos.peek().serverName;
      }
    }
    return host.split(":")[0];
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
    
    FragmentAssignInfo info = null;
    Iterator<FragmentAssignInfo> it = servingInfos.iterator();
    while (it.hasNext()) {
      info = it.next();
      if (info.serverName.equals(workerName)) {
        servingInfos.remove(info);
        break;
      }
    }
    if (info == null) {
      // error
    }
    info.fragmentNum = tempFrag.size();
    servingInfos.add(info);
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
    for (List<String> serverNames : DNSNameToHostsMap.values()) {
      if (rand.nextBoolean()) {
        return serverNames.get(rand.nextInt(serverNames.size()));
      }
    }
    return DNSNameToHostsMap.values().iterator().next().get(0);
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
      Fragment frag = unit.getFragment(scan.getTableId());
      serverName = this.getWorkerbyFrag(frag);
      if (map.containsKey(serverName)) {
        map.put(serverName, map.get(serverName)+1);
      } else {
        map.put(serverName, 1);
      }
    }
    PriorityQueue<FragmentAssignInfo> pq =
        new PriorityQueue<FragmentAssignInfo>();
    Iterator<Entry<String,Integer>> it = map.entrySet().iterator();
    Entry<String,Integer> e;
    while (it.hasNext()) {
      e = it.next();
      pq.add(new FragmentAssignInfo(e.getKey(), e.getValue()));
    }
    
//    Set<String> serverNames = DNSNameToHostsMap.keySet();
    Collection<List<String>> serverCollection = DNSNameToHostsMap.values();
    // 가장 많은 fragment를 담당하는 worker부터 online worker에 있는지 확인
    for (FragmentAssignInfo assignInfo : pq) {
      for (List<String> serverNames : serverCollection) {
        if (serverNames.contains(assignInfo.serverName)) {
          return assignInfo.serverName;
        }
      }
    }
    return null;
  }
}