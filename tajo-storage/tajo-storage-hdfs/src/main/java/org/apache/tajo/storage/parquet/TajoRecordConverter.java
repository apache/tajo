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

package org.apache.tajo.storage.parquet;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.*;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.GroupType;
import parquet.schema.Type;

import java.nio.ByteBuffer;

/**
 * Converter to convert a Parquet record into a Tajo Tuple.
 */
public class TajoRecordConverter extends GroupConverter {
  private final GroupType parquetSchema;
  private final Schema tajoReadSchema;
  private final int[] projectionMap;
  private final int tupleSize;

  private final Converter[] converters;

  private Tuple currentTuple;

  /**
   * Creates a new TajoRecordConverter.
   *
   * @param parquetSchema The Parquet schema of the projection.
   * @param tajoReadSchema The Tajo schema of the table.
   * @param projectionMap An array mapping the projection column to the column
   *                      index in the table.
   */
  public TajoRecordConverter(GroupType parquetSchema, Schema tajoReadSchema,
                             int[] projectionMap) {
    this.parquetSchema = parquetSchema;
    this.tajoReadSchema = tajoReadSchema;
    this.projectionMap = projectionMap;
    this.tupleSize = tajoReadSchema.size();

    // The projectionMap.length does not match parquetSchema.getFieldCount()
    // when the projection contains NULL_TYPE columns. We will skip over the
    // NULL_TYPE columns when we construct the converters and populate the
    // NULL_TYPE columns with NullDatums in start().
    int index = 0;
    this.converters = new Converter[parquetSchema.getFieldCount()];
    for (int i = 0; i < projectionMap.length; ++i) {
      final int projectionIndex = projectionMap[i];
      Column column = tajoReadSchema.getColumn(projectionIndex);
      if (column.getDataType().getType() == TajoDataTypes.Type.NULL_TYPE) {
        continue;
      }
      Type type = parquetSchema.getType(index);
      final int writeIndex = i;
      converters[index] = newConverter(column, type, new ParentValueContainer() {
        @Override
        void add(Object value) {
          TajoRecordConverter.this.set(writeIndex, value);
        }
      });
      ++index;
    }
  }

  private void set(int index, Object value) {
    currentTuple.put(index, (Datum)value);
  }

  private Converter newConverter(Column column, Type type,
                                 ParentValueContainer parent) {
    DataType dataType = column.getDataType();
    switch (dataType.getType()) {
      case BOOLEAN:
        return new FieldBooleanConverter(parent);
      case BIT:
        return new FieldBitConverter(parent);
      case CHAR:
        return new FieldCharConverter(parent);
      case INT2:
        return new FieldInt2Converter(parent);
      case INT4:
        return new FieldInt4Converter(parent);
      case INT8:
        return new FieldInt8Converter(parent);
      case FLOAT4:
        return new FieldFloat4Converter(parent);
      case FLOAT8:
        return new FieldFloat8Converter(parent);
      case INET4:
        return new FieldInet4Converter(parent);
      case INET6:
        throw new RuntimeException("No converter for INET6");
      case TEXT:
        return new FieldTextConverter(parent);
      case PROTOBUF:
        return new FieldProtobufConverter(parent, dataType);
      case BLOB:
        return new FieldBlobConverter(parent);
      case NULL_TYPE:
        throw new RuntimeException("No converter for NULL_TYPE.");
      default:
        throw new RuntimeException("Unsupported data type");
    }
  }

  /**
   * Gets the converter for a specific field.
   *
   * @param fieldIndex Index of the field in the projection.
   * @return The converter for the field.
   */
  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  /**
   * Called before processing fields. This method fills any fields that have
   * NULL values or have type NULL_TYPE with a NullDatum.
   */
  @Override
  public void start() {
    currentTuple = new VTuple(projectionMap.length);
  }

  /**
   * Called after all fields have been processed.
   */
  @Override
  public void end() {
    for (int i = 0; i < projectionMap.length; ++i) {
      final int projectionIndex = projectionMap[i];
      Column column = tajoReadSchema.getColumn(projectionIndex);
      if (column.getDataType().getType() == TajoDataTypes.Type.NULL_TYPE
          || currentTuple.isBlankOrNull(i)) {
        set(projectionIndex, NullDatum.get());
      }
    }
  }

  /**
   * Returns the current record converted by this converter.
   *
   * @return The current record.
   */
  public Tuple getCurrentRecord() {
    return currentTuple;
  }

  static abstract class ParentValueContainer {
    /**
     * Adds the value to the parent.
     *
     * @param value The value to add.
     */
    abstract void add(Object value);
  }

  static final class FieldBooleanConverter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldBooleanConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBoolean(boolean value) {
      parent.add(DatumFactory.createBool(value));
    }
  }

  static final class FieldBitConverter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldBitConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add(DatumFactory.createBit((byte)(value & 0xff)));
    }
  }

  static final class FieldCharConverter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldCharConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(DatumFactory.createChar(value.getBytes()));
    }
  }

  static final class FieldInt2Converter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldInt2Converter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add(DatumFactory.createInt2((short)value));
    }
  }

  static final class FieldInt4Converter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldInt4Converter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add(DatumFactory.createInt4(value));
    }
  }

  static final class FieldInt8Converter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldInt8Converter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addLong(long value) {
      parent.add(DatumFactory.createInt8(value));
    }

    @Override
    final public void addInt(int value) {
      parent.add(DatumFactory.createInt8(Long.valueOf(value)));
    }
  }

  static final class FieldFloat4Converter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldFloat4Converter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add(DatumFactory.createFloat4(Float.valueOf(value)));
    }

    @Override
    final public void addLong(long value) {
      parent.add(DatumFactory.createFloat4(Float.valueOf(value)));
    }

    @Override
    final public void addFloat(float value) {
      parent.add(DatumFactory.createFloat4(value));
    }
  }

  static final class FieldFloat8Converter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldFloat8Converter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add(DatumFactory.createFloat8(Double.valueOf(value)));
    }

    @Override
    final public void addLong(long value) {
      parent.add(DatumFactory.createFloat8(Double.valueOf(value)));
    }

    @Override
    final public void addFloat(float value) {
      parent.add(DatumFactory.createFloat8(Double.valueOf(value)));
    }

    @Override
    final public void addDouble(double value) {
      parent.add(DatumFactory.createFloat8(value));
    }
  }

  static final class FieldInet4Converter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldInet4Converter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(DatumFactory.createInet4(value.getBytes()));
    }
  }

  static final class FieldTextConverter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldTextConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(DatumFactory.createText(value.getBytes()));
    }
  }

  static final class FieldBlobConverter extends PrimitiveConverter {
    private final ParentValueContainer parent;

    public FieldBlobConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(new BlobDatum(ByteBuffer.wrap(value.getBytes())));
    }
  }

  static final class FieldProtobufConverter extends PrimitiveConverter {
    private final ParentValueContainer parent;
    private final DataType dataType;

    public FieldProtobufConverter(ParentValueContainer parent,
                                  DataType dataType) {
      this.parent = parent;
      this.dataType = dataType;
    }

    @Override
    final public void addBinary(Binary value) {
      try {
        ProtobufDatumFactory factory =
            ProtobufDatumFactory.get(dataType.getCode());
        Message.Builder builder = factory.newBuilder();
        builder.mergeFrom(value.getBytes());
        parent.add(factory.createDatum(builder));
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
