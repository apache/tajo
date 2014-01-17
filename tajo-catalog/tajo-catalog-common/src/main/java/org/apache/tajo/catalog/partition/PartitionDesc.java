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

package org.apache.tajo.catalog.partition;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PartitionDesc implements ProtoObject<CatalogProtos.PartitionDescProto>, Cloneable, GsonObject {
  @Expose protected CatalogProtos.PartitionsType partitionsType; //required
  @Expose protected Schema schema;
  @Expose protected int numPartitions; //optional
  @Expose protected List<Specifier> specifiers; //optional
  @Expose protected boolean isOmitValues = false; // optional;

  private CatalogProtos.PartitionDescProto.Builder builder = CatalogProtos.PartitionDescProto.newBuilder();

  public PartitionDesc() {
  }

  public PartitionDesc(PartitionDesc partition) {
    this();
    this.partitionsType = partition.partitionsType;
    this.schema = partition.schema;
    this.numPartitions = partition.numPartitions;
    this.specifiers = partition.specifiers;
  }

  public PartitionDesc(CatalogProtos.PartitionsType partitionsType, Column[] columns, int numPartitions,
                       List<Specifier> specifiers) {
    this();
    this.partitionsType = partitionsType;
    for (Column c : columns) {
      addColumn(c);
    }
    this.numPartitions = numPartitions;
    this.specifiers = specifiers;
  }

  public PartitionDesc(CatalogProtos.PartitionDescProto proto) {
    this.partitionsType = proto.getPartitionsType();
    this.schema = new Schema(proto.getSchema());
    this.numPartitions = proto.getNumPartitions();
    this.isOmitValues = proto.getIsOmitValues();
    if(proto.getSpecifiersList() != null) {
      this.specifiers = TUtil.newList();
      for(CatalogProtos.SpecifierProto specifier: proto.getSpecifiersList()) {
        this.specifiers.add(new Specifier(specifier));
      }
    }
  }

  public Schema getSchema() {
    return schema;
  }

  public List<Column> getColumns() {
    return ImmutableList.copyOf(schema.toArray());
  }

  public void setColumns(Collection<Column> columns) {
    this.schema = new Schema(columns.toArray(new Column[columns.size()]));
  }

  public synchronized void addColumn(Column column) {
    if (schema == null) {
      schema = new Schema();
    }
    schema.addColumn(column);
  }

  public synchronized void addSpecifier(Specifier specifier) {
    if(specifiers == null)
      specifiers = TUtil.newList();

    specifiers.add(specifier);
  }

  public CatalogProtos.PartitionsType getPartitionsType() {
    return partitionsType;
  }

  public void setPartitionsType(CatalogProtos.PartitionsType partitionsType) {
    this.partitionsType = partitionsType;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public void setNumPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  public List<Specifier> getSpecifiers() {
    return specifiers;
  }

  public void setSpecifiers(List<Specifier> specifiers) {
    this.specifiers = specifiers;
  }

  public void setOmitValues(boolean flag) {
    isOmitValues = flag;
  }

  public boolean isOmitValues() {
    return isOmitValues;
  }

  public boolean equals(Object o) {
    if (o instanceof PartitionDesc) {
      PartitionDesc another = (PartitionDesc) o;
      boolean eq = partitionsType == another.partitionsType;
      eq = eq && schema.equals(another.schema);
      eq = eq && numPartitions == another.numPartitions;
      eq = eq && TUtil.checkEquals(specifiers, another.specifiers);
      eq = eq && isOmitValues == another.isOmitValues;
      return eq;
    }
    return false;
  }

  public Object clone() throws CloneNotSupportedException {
    PartitionDesc copy = (PartitionDesc) super.clone();
    copy.builder = CatalogProtos.PartitionDescProto.newBuilder();
    copy.setPartitionsType(this.partitionsType);
    copy.schema = new Schema(schema.getProto());
    copy.setNumPartitions(this.numPartitions);
    copy.specifiers = new ArrayList<Specifier>(this.specifiers);
    copy.isOmitValues = isOmitValues;

    return copy;
  }

  @Override
  public CatalogProtos.PartitionDescProto getProto() {
    if (builder == null) {
      builder = CatalogProtos.PartitionDescProto.newBuilder();
    }
    if (this.partitionsType != null) {
      builder.setPartitionsType(this.partitionsType);
    }
    builder.setSchema(schema.getProto());
    builder.setNumPartitions(numPartitions);
    builder.setIsOmitValues(isOmitValues);
    if (this.specifiers != null) {
      for(Specifier specifier: specifiers) {
        builder.addSpecifiers(specifier.getProto());
      }
    }
    return builder.build();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("Partition Type: " + partitionsType.name()).append(", key=");
    sb.append(schema);
    return sb.toString();
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, PartitionDesc.class);

  }

  public static PartitionDesc fromJson(String strVal) {
    return strVal != null ? CatalogGsonHelper.fromJson(strVal, PartitionDesc.class) : null;
  }
}