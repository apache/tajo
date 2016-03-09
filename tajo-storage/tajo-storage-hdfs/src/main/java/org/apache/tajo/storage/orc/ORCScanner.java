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

package org.apache.tajo.storage.orc;

import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.memory.AggregatedMemoryContext;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.*;
import com.google.protobuf.InvalidProtocolBufferException;
import io.airlift.units.DataSize;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.*;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.storage.FileScanner;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.thirdparty.orc.HdfsOrcDataSource;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * OrcScanner for reading ORC files
 */
public class ORCScanner extends FileScanner {
  private static final Log LOG = LogFactory.getLog(ORCScanner.class);
  private OrcRecordReader recordReader;
  private Block[] blocks;
  private int currentPosInBatch = 0;
  private int batchSize = 0;
  private Tuple outTuple;
  private AggregatedMemoryContext aggrMemoryContext = new AggregatedMemoryContext();

  public ORCScanner(Configuration conf, final Schema schema, final TableMeta meta, final Fragment fragment) {
    super(conf, schema, meta, fragment);
  }

  private FileSystem fs;
  private FSDataInputStream fis;

  private static class ColumnInfo {
    TajoDataTypes.DataType type;
    int id;
  }

  /**
   * Temporary array for caching column info
   */
  private ColumnInfo [] targetColInfo;

  @Override
  public void init() throws IOException {
    OrcReader orcReader;
    DataSize maxMergeDistance = new DataSize(Double.parseDouble(meta.getProperty(StorageConstants.ORC_MAX_MERGE_DISTANCE,
            StorageConstants.DEFAULT_ORC_MAX_MERGE_DISTANCE)), DataSize.Unit.BYTE);
    DataSize maxReadSize = new DataSize(Double.parseDouble(meta.getProperty(StorageConstants.ORC_MAX_READ_BUFFER_SIZE,
        StorageConstants.DEFAULT_ORC_MAX_READ_BUFFER_SIZE)), DataSize.Unit.BYTE);

    if (targets == null) {
      targets = schema.toArray();
    }

    outTuple = new VTuple(targets.length);

    Path path = fragment.getPath();

    if(fs == null) {
      fs = FileScanner.getFileSystem((TajoConf)conf, path);
    }

    if(fis == null) {
      fis = fs.open(path);
    }

    OrcDataSource orcDataSource = new HdfsOrcDataSource(
        this.fragment.getPath().toString(),
        fis,
        fs.getFileStatus(path).getLen(),
        maxMergeDistance,
        maxReadSize);

    targetColInfo = new ColumnInfo[targets.length];
    for (int i=0; i<targets.length; i++) {
      ColumnInfo cinfo = new ColumnInfo();
      cinfo.type = targets[i].getDataType();
      cinfo.id = schema.getColumnId(targets[i].getQualifiedName());
      targetColInfo[i] = cinfo;
    }

    // creating blocks for buffering
    blocks = new Block[targetColInfo.length];

    Map<Integer, Type> columnMap = new HashMap<>();
    for (ColumnInfo colInfo: targetColInfo) {
      columnMap.put(colInfo.id, createFBtypeByTajoType(colInfo.type));
    }

    orcReader = new OrcReader(orcDataSource, new OrcMetadataReader(), maxMergeDistance, maxReadSize);

    TimeZone timezone = TimeZone.getTimeZone(meta.getProperty(StorageConstants.TIMEZONE,
      TajoConstants.DEFAULT_SYSTEM_TIMEZONE));

    // TODO: make OrcPredicate useful
    // presto-orc uses joda timezone, so it needs to be converted.
    recordReader = orcReader.createRecordReader(columnMap, OrcPredicate.TRUE,
        fragment.getStartKey(), fragment.getLength(), DateTimeZone.forTimeZone(timezone), aggrMemoryContext);

    super.init();
    LOG.debug("file fragment { path: " + fragment.getPath() +
      ", start offset: " + fragment.getStartKey() +
      ", length: " + fragment.getLength() + "}");
  }

  @Override
  public Tuple next() throws IOException {
    if (currentPosInBatch == batchSize) {
      getNextBatch();

      // EOF
      if (batchSize == -1) {
        return null;
      }
    }

    for (int i=0; i<targetColInfo.length; i++) {
      outTuple.put(i, createValueDatum(blocks[i], targetColInfo[i].type));
    }

    currentPosInBatch++;

    return outTuple;
  }

  private Type createFBtypeByTajoType(TajoDataTypes.DataType type) {
    switch(type.getType()) {
      case BOOLEAN:
        return BooleanType.BOOLEAN;

      case INT1:
      case INT2:
      case INT4:
      case INT8:
      case INET4:
      case NULL_TYPE: // meaningless
        return BigintType.BIGINT;

      case TIMESTAMP:
        return TimestampType.TIMESTAMP;

      case DATE:
        return DateType.DATE;

      case FLOAT4:
      case FLOAT8:
        return DoubleType.DOUBLE;

      case CHAR:
      case TEXT:
        return VarcharType.VARCHAR;

      case BLOB:
      case PROTOBUF:
        return VarbinaryType.VARBINARY;

      default:
        throw new TajoRuntimeException(new NotImplementedException(type.getType().name() + " for orc"));
    }
  }

  // TODO: support more types
  private Datum createValueDatum(Block block, TajoDataTypes.DataType type) {
    if (block.isNull(currentPosInBatch))
      return NullDatum.get();

    // NOTE: block.get***() methods are determined by the type size wich is in createFBtypeByTajoType()
    switch (type.getType()) {
      case INT1:
        return DatumFactory.createInt2((short)block.getLong(currentPosInBatch, 0));

      case INT2:
        return DatumFactory.createInt2((short)block.getLong(currentPosInBatch, 0));

      case INT4:
        return DatumFactory.createInt4((int)block.getLong(currentPosInBatch, 0));

      case INT8:
        return DatumFactory.createInt8(block.getLong(currentPosInBatch, 0));

      case FLOAT4:
        return DatumFactory.createFloat4((float)block.getDouble(currentPosInBatch, 0));

      case FLOAT8:
        return DatumFactory.createFloat8(block.getDouble(currentPosInBatch, 0));

      case BOOLEAN:
        return DatumFactory.createBool(block.getByte(currentPosInBatch, 0) != 0);

      case CHAR:
        return DatumFactory.createChar(block.getSlice(currentPosInBatch, 0,
            block.getLength(currentPosInBatch)).getBytes());

      case TEXT:
        return DatumFactory.createText(block.getSlice(currentPosInBatch, 0,
            block.getLength(currentPosInBatch)).getBytes());

      case BLOB:
        return DatumFactory.createBlob(block.getSlice(currentPosInBatch, 0,
            block.getLength(currentPosInBatch)).getBytes());

      case PROTOBUF:
        try {
          return ProtobufDatumFactory.createDatum(type, block.getSlice(currentPosInBatch, 0,
              block.getLength(currentPosInBatch)).getBytes());
        } catch (InvalidProtocolBufferException e) {
          LOG.error("ERROR", e);
          return NullDatum.get();
        }

      case TIMESTAMP:
        return DatumFactory.createTimestamp(
            DateTimeUtil.javaTimeToJulianTime(block.getLong(currentPosInBatch, 0)));

      case DATE:
        return DatumFactory.createDate(
            block.getInt(currentPosInBatch, 0) + DateTimeUtil.DAYS_FROM_JULIAN_TO_EPOCH);

      case INET4:
        return DatumFactory.createInet4((int)block.getLong(currentPosInBatch, 0));

      case NULL_TYPE:
        return NullDatum.get();

      default:
        throw new TajoRuntimeException(new NotImplementedException(type.getType().name() + " for orc"));
    }
  }

  /**
   * Fetch next batch from ORC file and write to block data structure as many as batch size
   *
   * @throws IOException
   */
  private void getNextBatch() throws IOException {
    batchSize = recordReader.nextBatch();

    // end of file
    if (batchSize == -1)
      return;

    for (int i=0; i<targetColInfo.length; i++) {
      blocks[i] = recordReader.readBlock(createFBtypeByTajoType(targetColInfo[i].type), targetColInfo[i].id);
    }

    currentPosInBatch = 0;
  }

  @Override
  public float getProgress() {
    if(!inited) return super.getProgress();

    return recordReader.getProgress();
  }

  @Override
  public void reset() throws IOException {
  }

  @Override
  public void close() throws IOException {
    if (recordReader != null) {
      recordReader.close();
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
  public void setFilter(EvalNode filter) {
    // TODO: implement it
  }

  @Override
  public boolean isSplittable() {
    return true;
  }
}
