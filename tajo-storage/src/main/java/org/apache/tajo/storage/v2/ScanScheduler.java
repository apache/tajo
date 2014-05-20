/**
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

package org.apache.tajo.storage.v2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.storage.v2.StorageManagerV2.StorgaeManagerContext;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.tajo.conf.TajoConf.ConfVars;

public final class ScanScheduler extends Thread {
  private static final Log LOG = LogFactory.getLog(ScanScheduler.class);

  private final Object scanQueueLock;
  private StorgaeManagerContext context;

  private Map<String, FileScannerV2> requestMap = new HashMap<String, FileScannerV2>();

  private final Map<Integer, DiskFileScanScheduler> diskFileScannerMap = new HashMap<Integer, DiskFileScanScheduler>();

  private Map<Integer, DiskDeviceInfo> diskDeviceInfoMap = new HashMap<Integer, DiskDeviceInfo>();

  private SortedSet<DiskMountInfo> diskMountInfos = new TreeSet<DiskMountInfo>();

  private AtomicBoolean stopped = new AtomicBoolean(false);

  private Random rand = new Random(System.currentTimeMillis());

  private Thread schedulerStatusReportThread;

  public ScanScheduler(StorgaeManagerContext context) {
    this.context = context;
    this.scanQueueLock = context.getScanQueueLock();

    try {
      List<DiskDeviceInfo> deviceInfos = DiskUtil.getDiskDeviceInfos();
      if (deviceInfos.size() == 0) {
        deviceInfos = DiskUtil.getDefaultDiskDeviceInfos();
      }
      for(DiskDeviceInfo eachInfo: deviceInfos) {
        LOG.info("Create DiskScanQueue:" + eachInfo.getName());
        diskDeviceInfoMap.put(eachInfo.getId(), eachInfo);

        diskMountInfos.addAll(eachInfo.getMountInfos());
      }

      initFileScanners();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    final int reportInterval = context.getConf().getIntVar(ConfVars.STORAGE_MANAGER_DISK_SCHEDULER_REPORT_INTERVAL);
    if(reportInterval  > 0) {
      schedulerStatusReportThread = new Thread() {
        public void run() {
          while (true) {
            try {
              Thread.sleep(reportInterval);
            } catch (InterruptedException e) {
              break;
            }
            synchronized (diskFileScannerMap) {
              for (DiskFileScanScheduler eachScheduler : diskFileScannerMap
                  .values()) {
                eachScheduler.printDiskSchedulerInfo();
              }
            }
          }
        }
      };

      schedulerStatusReportThread.start();
    }
  }

  public void run() {
    synchronized(scanQueueLock) {
      while(!stopped.get()) {
        FileScannerV2 fileScannerV2 = context.getScanQueue().poll();
        if(fileScannerV2 == null) {
          try {
            scanQueueLock.wait();
          } catch (InterruptedException e) {
            break;
          }
        } else {
          int diskId = fileScannerV2.getDiskId();

          int emptyDiskId = findEmptyDisk();
          if(emptyDiskId < 0) {
            if(diskId < 0 || diskId >= diskDeviceInfoMap.size()) {
              diskId = findDiskPartitionPath(fileScannerV2.getPath().toString());
              if(diskId < 0) {

                diskId = findMinQueueDisk();
                if(diskId < 0) {
                  diskId = rand.nextInt(diskDeviceInfoMap.size());
                }
              }
            }
          } else {
            diskId = emptyDiskId;
          }
          synchronized(diskFileScannerMap) {
            requestMap.put(fileScannerV2.getId(), fileScannerV2);
            DiskFileScanScheduler diskScheduler = diskFileScannerMap.get(diskId);
            fileScannerV2.setAllocatedDiskId(diskId);
            diskScheduler.requestScanFile(fileScannerV2);
          }
        }
      }
    }
  }

  private int findEmptyDisk() {
    synchronized(diskFileScannerMap) {
      for(DiskFileScanScheduler eachDiskScanner: diskFileScannerMap.values()) {
        int queueSize = eachDiskScanner.getTotalQueueSize();
        if(queueSize == 0) {
          return eachDiskScanner.getDiskId();
        }
      }
      return -1;
    }
  }
  
  private int findMinQueueDisk() {
    int minValue = Integer.MAX_VALUE;
    int minId = -1;
    synchronized(diskFileScannerMap) {
      for(DiskFileScanScheduler eachDiskScanner: diskFileScannerMap.values()) {
        int queueSize = eachDiskScanner.getTotalQueueSize();
        if(queueSize <= minValue) {
          minValue = queueSize;
          minId = eachDiskScanner.getDiskId();
        }
      }
    }

    return minId;
  }

  private int findDiskPartitionPath(String fullPath) {
    for (DiskMountInfo eachMountInfo : diskMountInfos) {
      if (fullPath.indexOf(eachMountInfo.getMountPath()) == 0) {
        return eachMountInfo.getDeviceId();
      }
    }

    return -1;
  }

  public void incrementReadBytes(int diskId, long[] readBytes) {
    diskFileScannerMap.get(diskId).incrementReadBytes(readBytes);
  }

  private void initFileScanners() {
    for(Integer eachId: diskDeviceInfoMap.keySet()) {
      DiskFileScanScheduler scanner = new DiskFileScanScheduler(context, diskDeviceInfoMap.get(eachId));
      scanner.start();

      diskFileScannerMap.put(eachId, scanner);
    }
  }

  public void stopScheduler() {
    stopped.set(true);
    for(DiskFileScanScheduler eachDiskScanner: diskFileScannerMap.values()) {
      eachDiskScanner.stopScan();
    }
    this.interrupt();
  }
}
