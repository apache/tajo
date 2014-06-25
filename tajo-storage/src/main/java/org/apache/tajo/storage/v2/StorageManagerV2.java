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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.AbstractStorageManager;
import org.apache.tajo.storage.NullScanner;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

public final class StorageManagerV2 extends AbstractStorageManager {
  private final Log LOG = LogFactory.getLog(StorageManagerV2.class);

	private Queue<FileScannerV2> scanQueue = new LinkedList<FileScannerV2>();
	
	private Object scanQueueLock = new Object();
	
	private Object scanDataLock = new Object();
	
	private ScanScheduler scanScheduler;
	
	private StorgaeManagerContext context;

  public StorageManagerV2(TajoConf conf) throws IOException {
    super(conf);
		context = new StorgaeManagerContext();
		scanScheduler = new ScanScheduler(context);
		scanScheduler.start();
    LOG.info("StorageManager v2 started...");
	}

  @Override
  public Class<? extends Scanner> getScannerClass(CatalogProtos.StoreType storeType) throws IOException {
    Class<? extends Scanner> scannerClass;

    String handlerName = storeType.name().toLowerCase();
    String handlerNameKey = handlerName + "_v2";

    scannerClass = SCANNER_HANDLER_CACHE.get(handlerNameKey);
    if (scannerClass == null) {
      scannerClass = conf.getClass(String.format("tajo.storage.scanner-handler.v2.%s.class",
          storeType.name().toLowerCase()), null, Scanner.class);
      SCANNER_HANDLER_CACHE.put(handlerNameKey, scannerClass);
    }

    return scannerClass;
  }

  @Override
  public Scanner getScanner(TableMeta meta, Schema schema, Fragment fragment, Schema target) throws IOException {
    if (fragment instanceof FileFragment) {
      FileFragment fileFragment = (FileFragment)fragment;
      if (fileFragment.getEndKey() == 0) {
        Scanner scanner = new NullScanner(conf, schema, meta, fileFragment);
        scanner.setTarget(target.toArray());

        return scanner;
      }
    }

    Scanner scanner;

    Class<? extends Scanner> scannerClass = getScannerClass(meta.getStoreType());
    if (scannerClass == null) {
      throw new IOException("Unknown Storage Type: " + meta.getStoreType());
    }

    scanner = newScannerInstance(scannerClass, conf, schema, meta, fragment);
    if (scanner.isProjectable()) {
      scanner.setTarget(target.toArray());
    }

    if(scanner instanceof FileScannerV2) {
      ((FileScannerV2)scanner).setStorageManagerContext(context);
    }
    return scanner;
  }

	public void requestFileScan(FileScannerV2 fileScanner) {
		synchronized(scanQueueLock) {
			scanQueue.offer(fileScanner);
			
			scanQueueLock.notifyAll();
		}
	}

	public StorgaeManagerContext getContext() {
		return context;
	}

  public class StorgaeManagerContext {
		public Object getScanQueueLock() {
			return scanQueueLock;
		}

		public Object getScanDataLock() {
			return scanDataLock;
		}
		
		public Queue<FileScannerV2> getScanQueue() {
			return scanQueue;
		}

		public int getMaxReadBytesPerScheduleSlot() {
			return conf.getIntVar(TajoConf.ConfVars.STORAGE_MANAGER_DISK_SCHEDULER_MAX_READ_BYTES_PER_SLOT);
		}

    public void requestFileScan(FileScannerV2 fileScanner) {
      StorageManagerV2.this.requestFileScan(fileScanner);
    }

    public TajoConf getConf() {
      return conf;
    }

    public void incrementReadBytes(int diskId, long[] readBytes) {
      scanScheduler.incrementReadBytes(diskId, readBytes);
    }
  }

	public void stop() {
		if(scanScheduler != null) {
			scanScheduler.stopScheduler();
		}
	}
}
