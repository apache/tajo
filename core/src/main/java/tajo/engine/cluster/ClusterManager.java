package tajo.engine.cluster;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.catalog.CatalogClient;
import tajo.catalog.FragmentServInfo;
import tajo.catalog.TableMetaImpl;
import tajo.catalog.proto.CatalogProtos.TableDescProto;
import tajo.conf.TajoConf;
import tajo.engine.MasterInterfaceProtos.ServerStatusProto;
import tajo.engine.MasterInterfaceProtos.ServerStatusProto.Disk;
import tajo.QueryUnitId;
import tajo.engine.exception.UnknownWorkerException;
import tajo.engine.ipc.protocolrecords.Fragment;
import tajo.rpc.Callback;
import tajo.rpc.RemoteException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

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

  public class WorkerResource implements Comparable<WorkerResource> {
    private String name;
    private Integer freeResource;

    public WorkerResource(String name, int freeResource) {
      this.name = name;
      this.freeResource = freeResource;
    }

    public String getName() {
      return this.name;
    }

    public int getFreeResource() {
      return this.freeResource;
    }

    public boolean hasFreeResource() {
      return this.freeResource > 0;
    }

    public boolean getResource() {
      if (hasFreeResource()) {
        freeResource--;
        return true;
      } else {
        return false;
      }
    }

    public void returnResource() {
      freeResource++;
    }

    @Override
    public int compareTo(WorkerResource workerResource) {
      return workerResource.freeResource - this.freeResource;
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof WorkerResource) {
        WorkerResource wr = (WorkerResource) o;
        if (wr.name.equals(this.name) && wr.freeResource == this.freeResource) {
          return true;
        }
      }
      return false;
    }

    @Override
    public String toString() {
      return name + " : " + freeResource;
    }
  }

  private final TajoConf conf;
  private final WorkerCommunicator wc;
  private CatalogClient catalog;
  private final LeafServerTracker tracker;

  private int clusterSize;
  private Map<String, List<String>> DNSNameToHostsMap;
  private Map<Fragment, FragmentServingInfo> servingInfoMap;
  private Random rand = new Random(System.currentTimeMillis());
  private Map<String, WorkerResource> resourcePool;
  private PriorityQueue<WorkerResource> sortedResources;
  private Set<String> failedWorkers;

  public ClusterManager(WorkerCommunicator wc, final TajoConf conf,
      LeafServerTracker tracker) throws IOException {
    this.wc = wc;
    this.conf = conf;
    this.tracker = tracker;
    this.DNSNameToHostsMap = Maps.newConcurrentMap();
    this.servingInfoMap = Maps.newConcurrentMap();
    this.resourcePool = Maps.newConcurrentMap();
    this.sortedResources = new PriorityQueue<WorkerResource>();
    this.failedWorkers = Sets.newHashSet();
    this.clusterSize = 0;
  }

  public void init() throws IOException {
    this.catalog = new CatalogClient(this.conf);
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
    WorkerResource wr;
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
      wr = resourcePool.remove(failed);
      sortedResources.remove(wr);
    }
  }

  public void resetResourceInfo()
      throws UnknownWorkerException, InterruptedException,
      ExecutionException {
    WorkerInfo info;
    WorkerResource wr;
    for (List<String> hosts : DNSNameToHostsMap.values()) {
      for (String host : hosts) {
        info = getWorkerInfo(host);
        //Preconditions.checkState(info.availableProcessors-info.taskNum >= 0);
        // TODO: correct free resource computation is required
        if (info.availableProcessors-info.taskNum > 0) {
          wr = new WorkerResource(host,
              (info.availableProcessors-info.taskNum) * 30);
          resourcePool.put(host, wr);
          sortedResources.add(wr);
        }
      }
    }
  }

  public boolean remainFreeResource() {
    return sortedResources.iterator().next().hasFreeResource();
  }

  public WorkerResource getResource(String worker) {
    return resourcePool.get(worker);
  }

  public void updateResourcePool(WorkerResource updated) {
    WorkerResource outdated = null;
    for (WorkerResource wr : sortedResources) {
      if (wr.getName().equals(updated.getName())) {
        outdated = wr;
        break;
      }
    }
    sortedResources.remove(outdated);
    sortedResources.add(updated);
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
  /*public String getRandomHost() {
    int n = rand.nextInt(resourcePool.size());
    Iterator<String> it = resourcePool.keySet().iterator();
    for (int i = 0; i < n-1; i++) {
      it.next();
    }
    String randomHost = it.next();
    this.getResource(randomHost);
    return randomHost;
  }*/

  public String getNextFreeHost() {
    return sortedResources.iterator().next().getName();
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
