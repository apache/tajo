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

package org.apache.tajo.storage.trevni;

import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.datum.BlobDatum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatumFactory;
import org.apache.tajo.storage.FileScanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnValues;
import org.apache.trevni.avro.HadoopInput;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.tajo.common.TajoDataTypes.DataType;

public class TrevniScanner extends FileScanner {
  private ColumnFileReader reader;
  private int [] projectionMap;
  private ColumnValues [] columns;

  public TrevniScanner(Configuration conf, Schema schema, TableMeta meta, FileFragment fragment) throws IOException {
    super(conf, schema, meta, fragment);
    reader = new ColumnFileReader(new HadoopInput(fragment.getPath(), conf));
  }

  @Override
  public void init() throws IOException {
    if (targets == null) {
      targets = schema.toArray();
    }

    prepareProjection(targets);

    columns = new ColumnValues[projectionMap.length];

    for (int i = 0; i < projectionMap.length; i++) {
      columns[i] = reader.getValues(projectionMap[i]);
    }

    super.init();
  }

  private void prepareProjection(Column [] targets) {
    projectionMap = new int[targets.length];
    int tid;
    for (int i = 0; i < targets.length; i++) {
      tid = schema.getColumnId(targets[i].getQualifiedName());
      projectionMap[i] = tid;
    }
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple = new VTuple(schema.size());

    if (!columns[0].hasNext()) {
      return null;
    }

    int tid; // column id of the original input schema
    for (int i = 0; i < projectionMap.length; i++) {
      tid = projectionMap[i];
      columns[i].startRow();
      DataType dataType = schema.getColumn(tid).getDataType();
      switch (dataType.getType()) {
        case BOOLEAN:
          tuple.put(tid,
              DatumFactory.createBool(((Integer)columns[i].nextValue()).byteValue()));
          break;
        case BIT:
          tuple.put(tid,
              DatumFactory.createBit(((Integer) columns[i].nextValue()).byteValue()));
          break;
        case CHAR:
          String str = (String) columns[i].nextValue();
          tuple.put(tid,
              DatumFactory.createChar(str));
          break;

        case INT2:
          tuple.put(tid,
              DatumFactory.createInt2(((Integer) columns[i].nextValue()).shortValue()));
          break;
        case INT4:
          tuple.put(tid,
              DatumFactory.createInt4((Integer) columns[i].nextValue()));
          break;

        case INT8:
          tuple.put(tid,
              DatumFactory.createInt8((Long) columns[i].nextValue()));
          break;

        case FLOAT4:
          tuple.put(tid,
              DatumFactory.createFloat4((Float) columns[i].nextValue()));
          break;

        case FLOAT8:
          tuple.put(tid,
              DatumFactory.createFloat8((Double) columns[i].nextValue()));
          break;

        case INET4:
          tuple.put(tid,
              DatumFactory.createInet4(((ByteBuffer) columns[i].nextValue()).array()));
          break;

        case TEXT:
          tuple.put(tid,
              DatumFactory.createText((String) columns[i].nextValue()));
          break;

        case PROTOBUF: {
          ProtobufDatumFactory factory = ProtobufDatumFactory.get(dataType.getCode());
          Message.Builder builder = factory.newBuilder();
          builder.mergeFrom(((ByteBuffer)columns[i].nextValue()).array());
          tuple.put(tid, factory.createDatum(builder));
          break;
        }

        case BLOB:
          tuple.put(tid,
              new BlobDatum(((ByteBuffer) columns[i].nextValue())));
          break;

        case NULL_TYPE:
          tuple.put(tid, NullDatum.get());
          break;

        default:
          throw new IOException("Unsupport data type");
      }
    }

    return tuple;
  }

  @Override
  public void reset() throws IOException {
    for (int i = 0; i < projectionMap.length; i++) {
      columns[i] = reader.getValues(projectionMap[i]);
    }
  }

  @Override
  public void close() throws IOException {
    reader.close();
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
    return false;
  }
}
