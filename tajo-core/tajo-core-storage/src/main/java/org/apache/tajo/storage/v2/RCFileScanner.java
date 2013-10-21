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
import org.apache.hadoop.io.LongWritable;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Fragment;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.rcfile.BytesRefArrayWritable;
import org.apache.tajo.storage.rcfile.ColumnProjectionUtils;
import org.apache.tajo.util.Bytes;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.util.ArrayList;

public class RCFileScanner extends FileScannerV2 {
  private static final Log LOG = LogFactory.getLog(RCFileScanner.class);

  private RCFile.Reader in;
  private long start;
  private long end;
  private boolean more = true;
  private LongWritable key;
  private BytesRefArrayWritable column;
  private Integer [] projectionMap;
  private ScheduledInputStream sin;
  private boolean first = true;
  private int maxBytesPerSchedule;

  public RCFileScanner(final Configuration conf,
                       final TableMeta meta,
                       final Fragment fragment) throws IOException {
    super(conf, meta, fragment);

    this.start = fragment.getStartOffset();
    this.end = start + fragment.getLength();
    key = new LongWritable();
    column = new BytesRefArrayWritable();
	}

  @Override
  protected Tuple nextTuple() throws IOException {
    if(first) {
      first = false;
      if (start > in.getPosition()) {
        in.sync(start); // sync to start
      }
      this.start = in.getPosition();
      more = start < end;
      if(!more) {
        return null;
      }
    }

    more = next(key);

    if (more) {
      column.clear();
      in.getCurrentRow(column);
    }

    if(more) {
      Tuple tuple = makeTuple();
      return tuple;
    } else {
      close();
      return null;
    }
  }

  private Tuple makeTuple() throws IOException {
    column.resetValid(schema.getColumnNum());
    Tuple tuple = new VTuple(schema.getColumnNum());
    int tid; // target column id
    for (int i = 0; i < projectionMap.length; i++) {
      tid = projectionMap[i];
      // if the column is byte[0], it presents a NULL value.
      if (column.get(tid).getLength() == 0) {
        tuple.put(tid, DatumFactory.createNullDatum());
      } else {
        switch (targets[i].getDataType().getType()) {
          case BOOLEAN:
            tuple.put(tid,
                DatumFactory.createBool(column.get(tid).getBytesCopy()[0]));
            break;
          case BIT:
            tuple.put(tid,
                DatumFactory.createBit(column.get(tid).getBytesCopy()[0]));
            break;
          case CHAR:
            byte[] buf = column.get(tid).getBytesCopy();
            tuple.put(tid,
                DatumFactory.createChar(buf));
            break;

          case INT2:
            tuple.put(tid,
                DatumFactory.createInt2(Bytes.toShort(
                    column.get(tid).getBytesCopy())));
            break;
          case INT4:
            tuple.put(tid,
                DatumFactory.createInt4(Bytes.toInt(
                    column.get(tid).getBytesCopy())));
            break;

          case INT8:
            tuple.put(tid,
                DatumFactory.createInt8(Bytes.toLong(
                    column.get(tid).getBytesCopy())));
            break;

          case FLOAT4:
            tuple.put(tid,
                DatumFactory.createFloat4(Bytes.toFloat(
                    column.get(tid).getBytesCopy())));
            break;

          case FLOAT8:
            tuple.put(tid,
                DatumFactory.createFloat8(Bytes.toDouble(
                    column.get(tid).getBytesCopy())));
            break;

          case INET4:
            tuple.put(tid,
                DatumFactory.createInet4(column.get(tid).getBytesCopy()));
            break;

          case TEXT:
            tuple.put(tid,
                DatumFactory.createText(
                    column.get(tid).getBytesCopy()));
            break;

          case BLOB:
            tuple.put(tid,
                DatumFactory.createBlob(column.get(tid).getBytesCopy()));
            break;

          default:
            throw new IOException("Unsupport data type");
        }
      }
    }

    return tuple;
  }

  @Override
  public void init() throws IOException {
    if (targets == null) {
      targets = schema.toArray();
    }

    prepareProjection(targets);

    super.init();
  }

  private void prepareProjection(Column[] targets) {
    projectionMap = new Integer[targets.length];
    int tid;
    for (int i = 0; i < targets.length; i++) {
      tid = schema.getColumnIdByName(targets[i].getColumnName());
      projectionMap[i] = tid;
    }
    ArrayList<Integer> projectionIdList = new ArrayList<Integer>(TUtil.newList(projectionMap));
    ColumnProjectionUtils.setReadColumnIDs(conf, projectionIdList);
  }

  @Override
  public void close() throws IOException {
    if(closed.get()) {
      return;
    }
    try {
      if(in != null) {
        in.close();
        in = null;
        sin = null;
      }
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }

    if(column != null) {
      column.clear();
      column = null;
    }
    super.close();
  }

  private boolean next(LongWritable key) throws IOException {
    if (!more) {
      return false;
    }

    more = in.next(key);
    if (!more) {
      return false;
    }

    long lastSeenSyncPos = in.lastSeenSyncPos();
    if (lastSeenSyncPos >= end) {
      more = false;
      return more;
    }
    return more;
  }

  @Override
  protected boolean initFirstScan(int maxBytesPerSchedule) throws IOException {
    synchronized(this) {
      first = true;
      this.maxBytesPerSchedule = maxBytesPerSchedule;
      if(sin == null) {
        sin = new ScheduledInputStream(
            fragment.getPath(),
            fs.open(fragment.getPath()),
            fragment.getStartOffset(),
            fragment.getLength(),
            fs.getLength(fragment.getPath()));

        this.in = new RCFile.Reader(fragment.getPath(), sin, fs, fs.getConf());
      }
    }
    return true;
  }

  @Override
  public boolean isStopScanScheduling() {
    if(sin != null && sin.IsEndOfStream()) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  protected boolean scanNext(int length) throws IOException {
    synchronized(this) {
      if(isClosed()) {
        return false;
      }
      return sin.readNext(length);
    }
  }


  @Override
  public boolean isFetchProcessing() {
    //TODO row group size
    if(sin != null && sin.getAvaliableSize() > maxBytesPerSchedule * 3) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  protected long getFilePosition() throws IOException {
    return in.getPosition();
  }

  @Override
  public void scannerReset() {
    if(in != null) {
      try {
        in.seek(0);
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }
    if(sin != null) {
      try {
        sin.seek(0);
        sin.reset();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }

  @Override
  public boolean isProjectable() {
    return true;
  }

  @Override
  public boolean isSelectable() {
    return false;
  }

  @Override
  public boolean isSplittable(){
    return true;
  }

  @Override
  protected long[] reportReadBytes() {
    if(sin == null) {
      return new long[]{0, 0};
    } else {
      return new long[]{sin.getTotalReadBytesForFetch(), sin.getTotalReadBytesFromDisk()};
    }
  }
}
