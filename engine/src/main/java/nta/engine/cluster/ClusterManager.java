package nta.engine.cluster;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import nta.catalog.CatalogClient;
import nta.catalog.FragmentServInfo;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.TableDescProto;
import nta.engine.MasterInterfaceProtos.ServerStatusProto;
import nta.engine.MasterInterfaceProtos.ServerStatusProto.Disk;
import nta.engine.QueryUnitId;
import nta.engine.exception.UnknownWorkerException;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.rpc.Callback;
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
    public int taskNum;

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
  private Map<String, List<String>> DNSNameToHostsMap;
  private Map<Fragment, FragmentServingInfo> servingInfoMap;
  private Random rand = new Random(System.currentTimeMillis());
  private Map<String, Integer> resourcePool;
  private Set<String> failedWorkers;

  public ClusterManager(WorkerCommunicator wc, Configuration conf,
      LeafServerTracker tracker) throws IOException {
    this.wc = wc;
    this.conf = conf;
    this.catalog = new CatalogClient(this.conf);
    this.tracker = tracker;
    this.DNSNameToHostsMap = Maps.newConcurrentMap();
    this.servingInfoMap = Maps.newConcurrentMap();
    this.resourcePool = Maps.newConcurrentMap();
    this.failedWorkers = Sets.newHashSet();
    this.clusterSize = 0;
  }

  public WorkerInfo getWorkerInfo(String workerName) throws RemoteException,
      InterruptedException, ExecutionException, UnknownWorkerException {
    Callback<ServerStatusProto> callback = wc.getServerStatus(workerName);
    for (int i = 0; i < 3 && !callback.isDone(); i++) {
      Thread.sleep(100);
    }
    if (callback.isDone()) {
      ServerStatusProto status = callback.get();
      ServerStatusProto.System system = status.getSystem();
      WorkerInfo info = new WorkerInfo();
      info.availableProcessors = system.getAvailableProcessors();
      info.freeMemory = system.getFreeMemory();
      info.totalMemory = system.getTotalMemory();
      info.taskNum = status.getTaskNum();

      for (Disk diskStatus : status.getDiskList()) {
        DiskInfo diskInfo = new DiskInfo();
        diskInfo.freeSpace = diskStatus.getFreeSpace();
        diskInfo.totalSpace = diskStatus.getTotalSpace();
        info.disks.add(diskInfo);
      }
      return info;
    } else {
      LOG.error("Failed to get the resource information!!");
      return null;
    }

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
      workers.removeAll(failedWorkers);
      if (workers.size() > 0) {
        clusterSize += workers.size();
        DNSNameToHostsMap.put(DNSName, workers);
      }
    }
    for (String failed : failedWorkers) {
      resourcePool.remove(failed);
    }
  }

  public void resetResourceInfo()
      throws UnknownWorkerException, InterruptedException,
      ExecutionException {
    WorkerInfo info;
    for (List<String> hosts : DNSNameToHostsMap.values()) {
      for (String host : hosts) {
        info = getWorkerInfo(host);
        //Preconditions.checkState(info.availableProcessors-info.taskNum >= 0);
        // TODO: correct free resource computation is required
        if (info.availableProcessors-info.taskNum > 0) {
          resourcePool.put(host,
              (info.availableProcessors-info.taskNum) * 30);
        }
      }
    }
  }

  public boolean existFreeResource() {
    return resourcePool.size() > 0;
  }

  public boolean hasFreeResource(String host) {
    return resourcePool.containsKey(host);
  }

  public void getResource(String host) {
    int resource = resourcePool.get(host);
    if (--resource == 0) {
      resourcePool.remove(host);
    } else {
      resourcePool.put(host, resource);
    }
  }

  public void addResource(String host) {
    if (resourcePool.containsKey(host)) {
      resourcePool.put(host, resourcePool.get(host).intValue()+1);
    } else {
      resourcePool.put(host, 1);
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

  public FragmentServingInfo getServingInfo(Fragment fragment) {
    return this.servingInfoMap.get(fragment);
  }
  
  private String getRandomWorkerNameOfHost(String host) {
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
  public String getRandomHost() {
    if (!existFreeResource()) {
      return null;
    }

    String result = null;
    while (result == null) {
      int n = rand.nextInt(DNSNameToHostsMap.size());
      Iterator<String> it = DNSNameToHostsMap.keySet().iterator();
      for (int i = 0; i < n-1; i++) {
        it.next();
      }
      String randomhost = it.next();
      String worker = getRandomWorkerNameOfHost(randomhost);
      if (hasFreeResource(worker)) {
        result = worker;
      }
    }
    this.getResource(result);
    return result;

    /*int n = rand.nextInt(resourcePool.size());
    Iterator<String> it = resourcePool.keySet().iterator();
    for (int i = 0; i < n-1; i++) {
      it.next();
    }
    String randomHost = it.next();
    this.getResource(randomHost);
    return randomHost;*/
  }

  public synchronized Set<String> getFailedWorkers() {
    return this.failedWorkers;
  }

  public synchronized void addFailedWorker(String worker) {
    this.failedWorkers.add(worker);
  }

  public synchronized boolean isExist(String worker) {
    return this.failedWorkers.contains(worker);
  }

  public synchronized void removeFailedWorker(String worker) {
    this.failedWorkers.remove(worker);
  }
}
