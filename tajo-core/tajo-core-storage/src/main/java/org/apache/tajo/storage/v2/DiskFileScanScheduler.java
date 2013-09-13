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

public class DiskFileScanScheduler extends Thread {
  private static final Log LOG = LogFactory.getLog(DiskFileScanScheduler.class);

	private Queue<FileScannerV2> requestQueue = new LinkedList<FileScannerV2>();

  Queue<FileScannerV2> fetchingScanners = new LinkedList<FileScannerV2>();

  private int scanConcurrency;

	private AtomicInteger numOfRunningScanners = new AtomicInteger(0);

	private Object requestQueueMonitor = new Object(); // c++ code style

	private StorageManagerV2.StorgaeManagerContext smContext;

	private DiskDeviceInfo diskDeviceInfo;

	private AtomicBoolean stopped = new AtomicBoolean(false);

  private long totalScanCount = 0;

  private FetchWaitingThread fetchWaitingThread;

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
          if(fileScanner.isFetchProcessing()) {
            fetchingScanners.add(fileScanner);
            synchronized(fetchingScanners) {
              fetchingScanners.notifyAll();
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
    public void run() {
      while(!stopped.get()) {
        FileScannerV2 scanner = null;
        synchronized(fetchingScanners) {
          scanner = fetchingScanners.poll();
          if(scanner == null) {
            try {
              fetchingScanners.wait();
              continue;
            } catch (InterruptedException e) {
              break;
            }
          }
        }
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          break;
        }
        synchronized(requestQueueMonitor) {
          requestQueue.offer(scanner);
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
}
