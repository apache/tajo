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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.FileFragment;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class FileScannerV2 implements Scanner {
  private static final Log LOG = LogFactory.getLog(FileScannerV2.class);

	protected AtomicBoolean closed = new AtomicBoolean(false);

	protected FileSystem fs;

  protected boolean inited = false;
  protected final Configuration conf;
  protected final TableMeta meta;
  protected final Schema schema;
  protected final FileFragment fragment;
  protected final int columnNum;
  protected Column[] targets;
  protected long totalScanTime = 0;
  protected int allocatedDiskId;

  protected StorageManagerV2.StorgaeManagerContext smContext;

  protected AtomicBoolean firstSchdeuled = new AtomicBoolean(true);

  protected abstract boolean scanNext(int length) throws IOException;

  protected abstract boolean initFirstScan(int maxBytesPerSchedule) throws IOException;

  protected abstract long getFilePosition() throws IOException;

  protected abstract Tuple nextTuple() throws IOException;

  public abstract boolean isFetchProcessing();

  public abstract boolean isStopScanScheduling();

  public abstract void scannerReset();

  protected abstract long[] reportReadBytes();

	public FileScannerV2(final Configuration conf,
                       final TableMeta meta,
                       final Schema schema,
                       final FileFragment fragment) throws IOException {
    this.conf = conf;
    this.meta = meta;
    this.schema = schema;
    this.fragment = fragment;
    this.columnNum = this.schema.size();

    this.fs = fragment.getPath().getFileSystem(conf);
	}

  public void init() throws IOException {
    closed.set(false);
    firstSchdeuled.set(true);

    if(!inited) {
      smContext.requestFileScan(this);
    }
    inited = true;
  }

  @Override
  public void reset() throws IOException {
    scannerReset();
    close();
    inited = false;
    init();
  }

  public void setAllocatedDiskId(int allocatedDiskId) {
    this.allocatedDiskId = allocatedDiskId;
  }

  public String getId() {
    return fragment.getPath().getName() + ":" + fragment.getStartKey() + ":" +
        fragment.getEndKey() + "_" + System.currentTimeMillis();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void setTarget(Column[] targets) {
    if (inited) {
      throw new IllegalStateException("Should be called before init()");
    }
    this.targets = targets;
  }

  public Path getPath() {
    return fragment.getPath();
  }

  public int getDiskId() {
    if(fragment.getDiskIds().length <= 0) {
      //LOG.warn("===> No DiskId:" + fragment.getPath() + ":" + fragment.getStartKey());
      return -1;
    } else {
      return fragment.getDiskIds()[0];
    }
  }

  public void setSearchCondition(Object expr) {
    if (inited) {
      throw new IllegalStateException("Should be called before init()");
    }
  }

  public void setStorageManagerContext(StorageManagerV2.StorgaeManagerContext context) {
    this.smContext = context;
  }

  public String toString() {
    return fragment.getPath() + ":" + fragment.getStartKey();
  }

  public void scan(int maxBytesPerSchedule) throws IOException {
    long startTime = System.currentTimeMillis();
    try {
    synchronized(firstSchdeuled) {
      if(firstSchdeuled.get()) {
        boolean moreData = initFirstScan(maxBytesPerSchedule);
        firstSchdeuled.set(false);
        firstSchdeuled.notifyAll();
        if(moreData) {
          smContext.requestFileScan(this);
        }
        return;
      }
    }
    boolean moreData = scanNext(maxBytesPerSchedule);

    if(moreData) {
      smContext.requestFileScan(this);
    }
    } finally {
      totalScanTime += System.currentTimeMillis() - startTime;
    }
  }

  @Override
  public void close() throws IOException {
    if(closed.get()) {
      return;
    }
    long[] readBytes = reportReadBytes();
    smContext.incrementReadBytes(allocatedDiskId, readBytes);
    closed.set(true);
    LOG.info(toString() + " closed, totalScanTime=" + totalScanTime);
  }

  public boolean isClosed() {
    return closed.get();
  }

  public Tuple next() throws IOException {
    synchronized(firstSchdeuled) {
      if(firstSchdeuled.get()) {
        try {
          firstSchdeuled.wait();
        } catch (InterruptedException e) {
        }
      }
    }
    return nextTuple();
  }
}
