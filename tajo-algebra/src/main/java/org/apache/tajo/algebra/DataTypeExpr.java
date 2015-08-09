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

package org.apache.tajo.algebra;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.util.TUtil;

public class DataTypeExpr extends Expr {
  @Expose @SerializedName("DataTypeName")
  String typeName;
  @Expose @SerializedName("LengthOrPrecision")
  Integer lengthOrPrecision;
  @Expose @SerializedName("Scale")
  Integer scale;
  @Expose @SerializedName("Record")
  RecordType recordType; // not null if the type is RECORD
  @Expose @SerializedName("Map")
  MapType mapType;

  public DataTypeExpr(String typeName) {
    super(OpType.DataType);
    this.typeName = typeName;
  }

  public DataTypeExpr(RecordType record) {
    super(OpType.DataType);
    this.typeName = Type.RECORD.name();
    this.recordType = record;
  }

  public DataTypeExpr(MapType map) {
    super(OpType.DataType);
    // RECORD = 51 in DataTypes.proto
    this.typeName = Type.MAP.name();
    this.mapType = map;
  }

  public String getTypeName() {
    return this.typeName;
  }

  public boolean isPrimitiveType() {
    return !this.isRecordType() && !isMapType();
  }

  public boolean isRecordType() {
    return this.typeName.equals(Type.RECORD.name());
  }

  public boolean isMapType() {
    return this.typeName.equals(Type.MAP.name());
  }

  public ColumnDefinition [] getNestedRecordTypes() {
    return recordType.schema;
  }

  public boolean hasLengthOrPrecision() {
    return lengthOrPrecision != null;
  }

  public void setLengthOrPrecision(int lengthOrPrecision) {
    this.lengthOrPrecision = lengthOrPrecision;
  }

  public Integer getLengthOrPrecision() {
    return this.lengthOrPrecision;
  }

  public boolean hasScale() {
    return this.scale != null;
  }

  public void setScale(int scale) {
    this.scale = scale;
  }

  public Integer getScale() {
    return this.scale;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(typeName, lengthOrPrecision, scale, recordType, mapType);
  }

  @Override
  boolean equalsTo(Expr expr) {
    DataTypeExpr another = (DataTypeExpr) expr;
    return typeName.equals(another.typeName) &&
        TUtil.checkEquals(lengthOrPrecision, another.lengthOrPrecision) &&
        TUtil.checkEquals(scale, another.scale) &&
        TUtil.checkEquals(recordType, another.recordType) &&
        TUtil.checkEquals(mapType, another.mapType);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    DataTypeExpr dataType = (DataTypeExpr) super.clone();
    dataType.typeName = typeName;
    // why we copy references? because they are all immutable.
    dataType.lengthOrPrecision = lengthOrPrecision;
    dataType.scale = scale;
    dataType.recordType = recordType;
    dataType.mapType = mapType;
    return dataType;
  }

  public static class RecordType implements JsonSerializable, Cloneable {
    @Expose @SerializedName("Schema")
    ColumnDefinition [] schema; // not null if the type is RECORD

    public RecordType(ColumnDefinition [] schema) {
      this.schema = schema;
    }

    @Override
    public String toJson() {
      return JsonHelper.toJson(this);
    }

    public Object clone() throws CloneNotSupportedException {
      RecordType newRecord = (RecordType) super.clone();
      newRecord.schema = this.schema;
      return newRecord;
    }
  }

  public static class MapType implements JsonSerializable, Cloneable {
    @Expose @SerializedName("KeyType")
    DataTypeExpr keyType;
    @Expose @SerializedName("ValueType")
    DataTypeExpr valueType;

    public MapType(DataTypeExpr key, DataTypeExpr value) {
      this.keyType = key;
      this.valueType = value;
    }

    @Override
    public String toJson() {
      return JsonHelper.toJson(this);
    }

    public Object clone() throws CloneNotSupportedException {
      MapType newMap = (MapType) super.clone();
      newMap.keyType = keyType;
      newMap.valueType = valueType;
      return newMap;
    }
  }
}
