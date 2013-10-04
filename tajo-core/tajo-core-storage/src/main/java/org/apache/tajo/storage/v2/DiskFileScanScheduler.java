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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DiskFileScanScheduler extends Thread {
  private static final Log LOG = LogFactory.getLog(DiskFileScanScheduler.class);

	private Queue<FileScannerV2> requestQueue = new LinkedList<FileScannerV2>();

  List<FileScannerV2> fetchingScanners = new ArrayList<FileScannerV2>();

  private int scanConcurrency;

	private AtomicInteger numOfRunningScanners = new AtomicInteger(0);

	private Object requestQueueMonitor = new Object(); // c++ code style

	private StorageManagerV2.StorgaeManagerContext smContext;

	private DiskDeviceInfo diskDeviceInfo;

	private AtomicBoolean stopped = new AtomicBoolean(false);

  private long totalScanCount = 0;

  private FetchWaitingThread fetchWaitingThread;

  private AtomicLong totalReadBytesForFetch = new AtomicLong(0);

  private AtomicLong totalReadBytesFromDisk = new AtomicLong(0);

  private long[] lastReportReadBytes;

  private long lastReportTime = 0;

	public DiskFileScanScheduler(
			StorageManagerV2.StorgaeManagerContext smContext,
			DiskDeviceInfo diskDeviceInfo) {
		super("DiskFileScanner:" + diskDeviceInfo);
		this.smContext = smContext;
		this.diskDeviceInfo = diskDeviceInfo;
		initScannerPool();
		this.fetchWaitingThread = new FetchWaitingThread();
		this.fetchWaitingThread.start();
	}

  public void incrementReadBytes(long[] readBytes) {
    totalReadBytesForFetch.addAndGet(readBytes[0]);
    totalReadBytesFromDisk.addAndGet(readBytes[1]);
  }

  public int getDiskId() {
    return diskDeviceInfo.getId();
  }

  public void run() {
    synchronized (requestQueueMonitor) {
      while(!stopped.get()) {
        if(isAllScannerRunning()) {
          try {
            requestQueueMonitor.wait(2000);
            continue;
          } catch (InterruptedException e) {
            break;
          }
        } else {
          FileScannerV2 fileScanner = requestQueue.poll();
          if(fileScanner == null) {
            try {
              requestQueueMonitor.wait(2000);
              continue;
            } catch (InterruptedException e) {
              break;
            }
          }
          if(fileScanner.isStopScanScheduling()) {
            LOG.info("Exit from Disk Queue:" + fileScanner.getId());
            continue;
          }
          if(fileScanner.isFetchProcessing()) {
            synchronized(fetchingScanners) {
              fetchingScanners.add(fileScanner);
              //fetchingScanners.notifyAll();
            }
          } else {
            numOfRunningScanners.incrementAndGet();
            FileScanRunner fileScanRunner = new FileScanRunner(
                DiskFileScanScheduler.this, smContext,
                fileScanner, requestQueueMonitor,
                numOfRunningScanners);
            totalScanCount++;
            fileScanRunner.start();
          }
        }
      }
    }
  }

	protected void requestScanFile(FileScannerV2 fileScannerV2) {
		synchronized (requestQueueMonitor) {
			requestQueue.offer(fileScannerV2);
			requestQueueMonitor.notifyAll();
		}
	}

  public class FetchWaitingThread extends Thread {
    List<FileScannerV2> workList = new ArrayList<FileScannerV2>(20);
    public void run() {
      while(!stopped.get()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          break;
        }
        workList.clear();
        synchronized(fetchingScanners) {
          workList.addAll(fetchingScanners);
          fetchingScanners.clear();
        }
        synchronized(requestQueueMonitor) {
          for(FileScannerV2 eachScanner: workList) {
            requestQueue.offer(eachScanner);
          }
          requestQueueMonitor.notifyAll();
        }
      }
    }
  }

	private void initScannerPool() {
		// TODO finally implements heuristic, currently set with property
		scanConcurrency = smContext.getConf().getInt("tajo.storage.manager.concurrency.perDisk", 1);
	}

  public int getTotalQueueSize() {
      return requestQueue.size();
  }

  boolean isAllScannerRunning() {
    return numOfRunningScanners.get() >= scanConcurrency;
  }

  public long getTotalScanCount() {
    return totalScanCount;
  }

	public void stopScan() {
		stopped.set(true);
		if (fetchWaitingThread != null) {
      fetchWaitingThread.interrupt();
		}

		this.interrupt();
	}

  public void printDiskSchedulerInfo() {
    long currentReadBytes[] = new long[]{totalReadBytesForFetch.get(), totalReadBytesFromDisk.get()};
    int[] throughput = new int[2];
    if(lastReportTime != 0 && lastReportReadBytes != null) {
      int sec = (int)((System.currentTimeMillis() - lastReportTime)/1000);
      throughput[0] = (int)((currentReadBytes[0] - lastReportReadBytes[0])/sec);
      throughput[1] = (int)((currentReadBytes[1] - lastReportReadBytes[1])/sec);
    }
    lastReportTime = System.currentTimeMillis();

    LOG.info("===>" + DiskFileScanScheduler.this.diskDeviceInfo
        + ", request=" + requestQueue.size()
        + ", fetching=" + fetchingScanners.size()
        + ", running=" + numOfRunningScanners.get()
        + ", totalScan=" + totalScanCount
        + ", FetchThroughput=" + throughput[0]/1024 + "KB"
        + ", DiskScanThroughput=" + throughput[1]/1024 + "KB");

    lastReportReadBytes = currentReadBytes;
  }
}
