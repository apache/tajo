/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.master.cluster;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import tajo.catalog.CatalogService;
import tajo.catalog.proto.CatalogProtos.TableDescProto;
import tajo.engine.MasterWorkerProtos.ServerStatusProto;
import tajo.engine.MasterWorkerProtos.ServerStatusProto.Disk;
import tajo.engine.exception.UnknownWorkerException;
import tajo.rpc.Callback;
import tajo.rpc.RemoteException;
import tajo.storage.Fragment;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class ClusterManager extends AbstractService {
  private final Log LOG = LogFactory.getLog(ClusterManager.class);

  private final Configuration conf;
  private final WorkerTracker tracker;
  private final WorkerCommunicator wc;
  private final CatalogService catalog;
  private final EventHandler eventHandler;

  private int clusterSize;
  private final Map<String, List<String>> DNSNameToHostsMap;
  private final Map<Fragment, FragmentServingInfo> servingInfoMap;
  private final Random rand = new Random(System.currentTimeMillis());
  private final Map<String, WorkerResource> resourcePool;
  private final PriorityQueue<WorkerResource> sortedResources;
  private final Set<String> failedWorkers;

  protected Thread eventHandlingThread;
  private volatile boolean stopped;

  public ClusterManager(final Configuration conf,
                        final WorkerCommunicator wc,
                        final WorkerTracker tracker,
                        final CatalogService catalog,
                        final EventHandler eventHandler)
      throws IOException {
    super(ClusterManager.class.getName());
    this.conf = conf;
    this.wc = wc;
    this.tracker = tracker;
    this.catalog = catalog;
    this.eventHandler = eventHandler;

    this.clusterSize = 0;
    this.DNSNameToHostsMap = Maps.newConcurrentMap();
    this.servingInfoMap = Maps.newConcurrentMap();
    this.resourcePool = Maps.newConcurrentMap();
    this.sortedResources = new PriorityQueue<WorkerResource>();
    this.failedWorkers = Sets.newHashSet();
  }

  public void init(Configuration conf) {
    updateOnlineWorker();
    try {
      resetResourceInfo();
    } catch (Exception e) {
      LOG.error(e);
      stopped = true;
    }
    super.init(conf);
  }

  public void start() {
    super.start();
  }

  public void stop() {
    super.stop();
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
        workers = new ArrayList<>();
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

  public String getNextFreeHost() {
    return sortedResources.iterator().next().getName();
  }

  public synchronized Set<String> getFailedWorkers() {
    return this.failedWorkers;
  }

  public synchronized void addFailedWorker(String worker) {
    this.failedWorkers.add(worker);
  }

  public void allocateSlot(String workerName) {
    WorkerResource wr = getResource(workerName);
    wr.getResource();
    updateResourcePool(wr);
  }

  public void freeSlot(String workerName) {
    WorkerResource wr = getResource(workerName);
    wr.returnResource();
    updateResourcePool(wr);
  }

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
}
