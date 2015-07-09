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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.*;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.FileScanner;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.fragment.Fragment;
import com.facebook.presto.orc.*;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import org.apache.tajo.storage.thirdparty.orc.HdfsOrcDataSource;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * OrcScanner for reading ORC files
 */
public class OrcScanner extends FileScanner {
  private static final Log LOG = LogFactory.getLog(OrcScanner.class);
  private OrcRecordReader recordReader;
  private Vector [] vectors;
  private int currentPosInBatch = 0;
  private int batchSize = 0;

  public OrcScanner(Configuration conf, final Schema schema, final TableMeta meta, final Fragment fragment) {
    super(conf, schema, meta, fragment);
  }

  private Vector createOrcVector(TajoDataTypes.Type type) {
    switch (type) {
      case INT1: case INT2: case INT4: case INT8:
      case UINT1: case UINT2: case UINT4: case UINT8:
      case TIMESTAMP:
      case DATE:
        return new LongVector();

      case FLOAT4:
      case FLOAT8:
        return new DoubleVector();

      case BOOLEAN:
        return new BooleanVector();

      case BLOB:
      case TEXT:
        return new SliceVector();

      default:
        throw new UnsupportedException("This data type is not supported currently: "+type.toString());
    }
  }

  private FileSystem fs;
  private FSDataInputStream fis;

  private static class ColumnInfo {
    TajoDataTypes.Type type;
    int id;
  }

  /**
   * Temporary array for caching column info
   */
  private ColumnInfo [] targetColInfo;

  @Override
  public void init() throws IOException {
    OrcReader orcReader;

    if (targets == null) {
      targets = schema.toArray();
    }

    super.init();

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
        Integer.parseInt(meta.getOption(StorageConstants.ORC_MAX_MERGE_DISTANCE,
          StorageConstants.DEFAULT_ORC_MAX_MERGE_DISTANCE)));

    targetColInfo = new ColumnInfo[targets.length];
    for (int i=0; i<targets.length; i++) {
      ColumnInfo cinfo = new ColumnInfo();
      cinfo.type = targets[i].getDataType().getType();
      cinfo.id = schema.getColumnId(targets[i].getQualifiedName());
      targetColInfo[i] = cinfo;
    }

    // creating vectors for buffering
    vectors = new Vector[targetColInfo.length];
    for (int i=0; i<targetColInfo.length; i++) {
      vectors[i] = createOrcVector(targetColInfo[i].type);
    }

    Set<Integer> columnSet = new HashSet<Integer>();
    for (ColumnInfo colInfo: targetColInfo) {
      columnSet.add(colInfo.id);
    }

    orcReader = new OrcReader(orcDataSource, new OrcMetadataReader());

    // TODO: make OrcPredicate useful
    // TODO: TimeZone should be from conf
    // TODO: it might be splittable
    recordReader = orcReader.createRecordReader(columnSet, OrcPredicate.TRUE,
        fragment.getStartKey(), fragment.getLength(), DateTimeZone.getDefault());

    LOG.debug("file fragment { path: " + fragment.getPath() +
      ", start offset: " + fragment.getStartKey() +
      ", length: " + fragment.getLength() + "}");

    getNextBatch();
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

    Tuple tuple = new VTuple(schema.size());

    for (int i=0; i<targetColInfo.length; i++) {
      tuple.put(targetColInfo[i].id, createValueDatum(vectors[i], targetColInfo[i].type));
    }

    currentPosInBatch++;

    return tuple;
  }

  // TODO: support more types
  private Datum createValueDatum(Vector vector, TajoDataTypes.Type type) {
    switch (type) {
      case INT1:
      case UINT1:
      case INT2:
      case UINT2:
        return new Int2Datum((short)((LongVector)vector).vector[currentPosInBatch]);

      case INT4:
      case UINT4:
        return new Int4Datum((int)((LongVector)vector).vector[currentPosInBatch]);

      case INT8:
      case UINT8:
        return new Int8Datum(((LongVector)vector).vector[currentPosInBatch]);

      case FLOAT4:
        return new Float4Datum((float)((DoubleVector)vector).vector[currentPosInBatch]);

      case FLOAT8:
        return new Float8Datum(((DoubleVector)vector).vector[currentPosInBatch]);

      case BOOLEAN:
        return ((BooleanVector)vector).vector[currentPosInBatch] ? BooleanDatum.TRUE : BooleanDatum.FALSE;

      case TEXT:
        return new TextDatum(((SliceVector)vector).vector[currentPosInBatch].getBytes());

      case BLOB:
        return new BlobDatum(((SliceVector)vector).vector[currentPosInBatch].getBytes());

      case TIMESTAMP:
        return new TimestampDatum(DateTimeUtil.javaTimeToJulianTime(((LongVector) vector).vector[currentPosInBatch]));

      case DATE:
        return new DateDatum((int)((LongVector)vector).vector[currentPosInBatch] + DateTimeUtil.DAYS_FROM_JULIAN_TO_EPOCH);

      default:
        throw new UnsupportedException("This data type is not supported currently: "+type.toString());
    }
  }

  /**
   * Fetch next batch from ORC file to vectors as many as batch size
   *
   * @throws IOException
   */
  private void getNextBatch() throws IOException {
    batchSize = recordReader.nextBatch();

    for (int i=0; i<targetColInfo.length; i++) {
      recordReader.readVector(targetColInfo[i].id, vectors[i]);
    }

    currentPosInBatch = 0;
  }

  @Override
  public float getProgress() {
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
  public boolean isSplittable() {
    return true;
  }
}
