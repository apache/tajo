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

import java.util.concurrent.atomic.AtomicInteger;

public class FileScanRunner extends Thread {
  private static final Log LOG = LogFactory.getLog(FileScanRunner.class);

  StorageManagerV2.StorgaeManagerContext smContext;
	FileScannerV2 fileScanner;
	Object requestQueueMonitor;
	AtomicInteger numOfRunningScanners;
	DiskFileScanScheduler diskFileScanScheduler;
	
	int maxReadBytes;
	
	public FileScanRunner(DiskFileScanScheduler diskFileScanScheduler, 
			StorageManagerV2.StorgaeManagerContext smContext,
      FileScannerV2 fileScanner, Object requestQueueMonitor,
			AtomicInteger numOfRunningScanners) {
		super("FileScanRunner:" + fileScanner.getId());
		this.diskFileScanScheduler = diskFileScanScheduler;
		this.fileScanner = fileScanner;
		this.smContext = smContext;
		this.requestQueueMonitor = requestQueueMonitor;
		this.numOfRunningScanners = numOfRunningScanners;
		
		this.maxReadBytes = smContext.getMaxReadBytesPerScheduleSlot();
	}

	public void run() {
    try {
//      long startTime = System.currentTimeMillis();
//      boolean fetching = fileScanner.isFetchProcessing();
      fileScanner.scan(maxReadBytes);
//      if(diskFileScanScheduler.getDiskId() == 1) {
//        LOG.info("========>" + diskFileScanScheduler.getDiskId() + "," + fileScanner.getId() +
//            ",fetching=" + fetching +
//            ", scanTime:" + (System.currentTimeMillis() - startTime) + " ms");
//      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      synchronized(requestQueueMonitor) {
        numOfRunningScanners.decrementAndGet();
        requestQueueMonitor.notifyAll();
      }
    }
	}
}
