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

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.exception.UnimplementedException;
import org.apache.tajo.storage.BinarySerializerDeserializer;
import org.apache.tajo.storage.SerializerDeserializer;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.directmem.RowOrientedRowBlock;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.rcfile.BytesRefArrayWritable;
import org.apache.tajo.storage.rcfile.ColumnProjectionUtils;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.util.ArrayList;

public class RCFileScanner extends FileScannerV2 {
  private static final Log LOG = LogFactory.getLog(RCFileScanner.class);
  public static final String SERDE = "rcfile.serde";
  public static final String NULL = "rcfile.null";

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
  private SerializerDeserializer serde;
  private byte[] nullChars;
  private Object lock = new Object();

  public RCFileScanner(final Configuration conf, final Schema schema, final TableMeta meta, final FileFragment fragment)
      throws IOException {
    super(conf, meta, schema, fragment);

    this.start = fragment.getStartKey();
    this.end = start + fragment.getEndKey();
    key = new LongWritable();
    column = new BytesRefArrayWritable();

    String nullCharacters = StringEscapeUtils.unescapeJava(this.meta.getOption(NULL, NullDatum.DEFAULT_TEXT));
    if (StringUtils.isEmpty(nullCharacters)) {
      nullChars = NullDatum.get().asTextBytes();
    } else {
      nullChars = nullCharacters.getBytes();
    }
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
    Tuple tuple = new VTuple(schema.size());
    synchronized (lock) {
      column.resetValid(schema.size());
      int tid; // target column id
      for (int i = 0; i < projectionMap.length; i++) {
        tid = projectionMap[i];

        byte[] bytes = column.get(tid).getBytesCopy();
        Datum datum = serde.deserialize(targets[i], bytes, 0, bytes.length, nullChars);
        tuple.put(tid, datum);
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

  @Override
  public boolean next(RowOrientedRowBlock block) throws IOException {
    throw new UnimplementedException("next(RowOrientedRowBlock)");
  }

  private void prepareProjection(Column[] targets) {
    projectionMap = new Integer[targets.length];
    int tid;
    for (int i = 0; i < targets.length; i++) {
      tid = schema.getColumnIdByName(targets[i].getSimpleName());
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
    synchronized(lock) {
      first = true;
      this.maxBytesPerSchedule = maxBytesPerSchedule;
      if(sin == null) {
        sin = new ScheduledInputStream(
            fragment.getPath(),
            fs.open(fragment.getPath()),
            fragment.getStartKey(),
            fragment.getEndKey(),
            fs.getLength(fragment.getPath()));

        this.in = new RCFile.Reader(fragment.getPath(), sin, fs, fs.getConf());

        Text text = this.in.getMetadata().get(new Text(SERDE));

        try {
          String serdeClass;
          if(text != null && !text.toString().isEmpty()){
            serdeClass = text.toString();
          } else{
            serdeClass = this.meta.getOption(SERDE, BinarySerializerDeserializer.class.getName());
          }
          serde = (SerializerDeserializer) Class.forName(serdeClass).newInstance();
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
          throw new IOException(e);
        }
      }
    }
    return true;
  }

  @Override
  public boolean isStopScanScheduling() {
    if(sin != null && sin.isEndOfStream()) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  protected boolean scanNext(int length) throws IOException {
    synchronized(lock) {
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
