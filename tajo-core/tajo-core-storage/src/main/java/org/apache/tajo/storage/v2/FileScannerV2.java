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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.storage.Fragment;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class FileScannerV2 implements Scanner {
  private static final Log LOG = LogFactory.getLog(FileScannerV2.class);

  protected AtomicBoolean fetchProcessing = new AtomicBoolean(false);

	protected AtomicBoolean closed = new AtomicBoolean(false);

	protected FileSystem fs;

  protected boolean inited = false;
  protected final Configuration conf;
  protected final TableMeta meta;
  protected final Schema schema;
  protected final Fragment fragment;
  protected final int columnNum;
  protected Column[] targets;

  protected StorageManagerV2.StorgaeManagerContext smContext;

  protected boolean firstSchdeuled = true;

  protected Queue<Tuple> tuplePool;

  AtomicInteger tuplePoolMemory = new AtomicInteger();

  protected abstract Tuple getNextTuple() throws IOException;

  protected abstract void initFirstScan() throws IOException;

  protected abstract long getFilePosition() throws IOException;

	public FileScannerV2(final Configuration conf,
                       final TableMeta meta,
                       final Fragment fragment) throws IOException {
    this.conf = conf;
    this.meta = meta;
    this.schema = meta.getSchema();
    this.fragment = fragment;
    this.columnNum = this.schema.getColumnNum();

    this.fs = fragment.getPath().getFileSystem(conf);

    tuplePool = new ConcurrentLinkedQueue<Tuple>();
	}

  public void init() throws IOException {
    closed.set(false);
    fetchProcessing.set(false);
    firstSchdeuled = true;
    //tuplePoolIndex = 0;
    if(tuplePool == null) {
      tuplePool = new ConcurrentLinkedQueue<Tuple>();
    }
    tuplePool.clear();

    if(!inited) {
      smContext.requestFileScan(this);
    }
    inited = true;
  }

  @Override
  public void reset() throws IOException {
    close();
    inited = false;

    init();
  }

  public String getId() {
    return fragment.getPath().toString() + ":" + fragment.getStartOffset() + ":" +
        fragment.getLength() + "_" + System.currentTimeMillis();
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
      //LOG.warn("===> No DiskId:" + fragment.getPath() + ":" + fragment.getStartOffset());
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

  public boolean isFetchProcessing() {
//    return fetchProcessing.get();
    return tuplePoolMemory.get() > 16 * 1024 * 1024;
  }

  long lastScanScheduleTime;

  public String toString() {
    return fragment.getPath() + ":" + fragment.getStartOffset();
  }

  public void scan(int maxBytesPerSchedule) throws IOException {
    if(firstSchdeuled) {
      initFirstScan();
      firstSchdeuled = false;
    }
    long scanStartPos = getFilePosition();
    int recordCount = 0;
    while(true) {
      Tuple tuple = getNextTuple();
      if(tuple == null) {
        break;
      }
      tuplePoolMemory.addAndGet(tuple.size());
      tuplePool.offer(tuple);
      recordCount++;
      if(recordCount % 1000 == 0) {
        if(getFilePosition() - scanStartPos >= maxBytesPerSchedule) {
          break;
        } else {
          synchronized(tuplePool) {
            tuplePool.notifyAll();
          }
        }
      }
    }
    if(tuplePool != null) {
      synchronized(tuplePool) {
        tuplePool.notifyAll();
      }
    }
    if(!isClosed()) {
      smContext.requestFileScan(this);
    }
  }

  public void waitScanStart() {
    //for test
    synchronized(fetchProcessing) {
      try {
        fetchProcessing.wait();
      } catch (InterruptedException e) {
      }
    }
  }

  @Override
  public void close() throws IOException {
    if(closed.get()) {
      return;
    }
    closed.set(true);

    synchronized(tuplePool) {
      tuplePool.notifyAll();
    }
    LOG.info(toString() + " closed");
  }

  public boolean isClosed() {
    return closed.get();
  }

  public Tuple next() throws IOException {
    if(isClosed() && tuplePool == null) {
      return null;
    }
    while(true) {
      Tuple tuple = tuplePool.poll();
      if(tuple == null) {
        if(isClosed()) {
          tuplePool.clear();
          tuplePool = null;
          return null;
        }
        synchronized(tuplePool) {
          try {
            tuplePool.wait();
          } catch (InterruptedException e) {
            break;
          }
        }
      } else {
        tuplePoolMemory.addAndGet(0 - tuple.size());
        return tuple;
      }
    }

    return null;
  }
}
