package nta.engine.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;

import nta.catalog.CatalogClient;
import nta.catalog.FragmentServInfo;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.TableDescProto;
import nta.engine.QueryUnitId;
import nta.engine.cluster.LeafServerStatusProtos.ServerStatusProto;
import nta.engine.cluster.LeafServerStatusProtos.ServerStatusProto.Disk;
import nta.engine.exception.UnknownWorkerException;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.rpc.RemoteException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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

  public List<String> getOnlineWorker() throws Exception {
    return tracker.getMembers();
  }

  public String getWorkerbyFrag(Fragment fragment) throws Exception {
    return workerLoc.get(fragment);
  }

  public Set<Fragment> getFragbyWorker(String workerName) throws Exception {
    return fragLoc.get(workerName);
  }

  public void updateAllFragmentServingInfo(List<String> onlineServers)
      throws Exception {
    fragLoc.clear();
    workerLoc.clear();
    
    Iterator<String> it = catalog.getAllTableNames().iterator();
    List<FragmentServInfo> locInfos;
    List<FragmentServInfo> servInfos;
    int index = 0;
    StringTokenizer tokenizer;
    while (it.hasNext()) {
      TableDescProto td = (TableDescProto) catalog.getTableDesc(it.next())
          .getProto();
      locInfos = getFragmentLocInfo(td);
      servInfos = new ArrayList<FragmentServInfo>();

      // TODO: select the proper online server
      for (FragmentServInfo locInfo : locInfos) {
        // round robin
        if (index == onlineServers.size()) {
          index = 0;
        }
        String workerName = onlineServers.get(index++);
        tokenizer = new StringTokenizer(workerName, ":");
        locInfo.setHost(tokenizer.nextToken(),
            Integer.valueOf(tokenizer.nextToken()));
        servInfos.add(locInfo);

        setFragLoc(locInfo, workerName);
      }
    }
  }

  private void setFragLoc(FragmentServInfo servInfo, String workerName) {
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
}