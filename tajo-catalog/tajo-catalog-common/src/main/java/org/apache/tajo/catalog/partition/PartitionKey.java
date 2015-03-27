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

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TUtil;


/**
 * This presents column name and partition value pairs of column partitioned table.
 *
 * For example, consider you have a partitioned table as follows:
 *
 * create external table table1 (id text, name text) PARTITION BY COLUMN (dt text, phone text,
 * gender text) USING RCFILE LOCATION '/tajo/data/table1';
 *
 * Then, its data will be stored on HDFS as follows:
 * - /tajo/data/table1/dt=20150301/phone=1300/gender=m
 * - /tajo/data/table1/dt=20150301/phone=1300/gender=f
 * - /tajo/data/table1/dt=20150302/phone=1500/gender=m
 * - /tajo/data/table1/dt=20150302/phone=1500/gender=f
 *
 * In such as above, first directory can be presented with this class as follows:
 * The first pair: column name = dt, partition value = 20150301
 * The second pair: column name = phone, partition value = 1300
 * The thris pair: column name = gender, partition value = m
 *
 */
public class PartitionKey implements ProtoObject<CatalogProtos.PartitionKeyProto>, Cloneable, GsonObject {
  @Expose protected String columnName;                       // required
  @Expose protected String partitionValue;                      // required

  private CatalogProtos.PartitionKeyProto.Builder builder = CatalogProtos.PartitionKeyProto.newBuilder();

  public PartitionKey() {
  }

  public PartitionKey(String columnName, String partitionValue) {
    this.columnName = columnName;
    this.partitionValue = partitionValue;
  }

  public PartitionKey(PartitionKey partition) {
    this.columnName = partition.columnName;
    this.partitionValue = partition.partitionValue;
  }

  public PartitionKey(CatalogProtos.PartitionKeyProto proto) {
    if (proto.hasColumnName()) {
      this.columnName = proto.getColumnName();
    }
    if (proto.hasPartitionValue()) {
      this.partitionValue = proto.getPartitionValue();
    }
  }

  public String getPartitionValue() {
    return partitionValue;
  }

  public void setPartitionValue(String partitionValue) {
    this.partitionValue = partitionValue;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public int hashCode() {
    return Objects.hashCode(partitionValue, columnName);
  }

  public boolean equals(Object o) {
    if (o instanceof PartitionKey) {
      PartitionKey another = (PartitionKey) o;
      return TUtil.checkEquals(columnName, another.columnName) &&
        TUtil.checkEquals(partitionValue, another.partitionValue);
    }
    return false;
  }

  @Override
  public CatalogProtos.PartitionKeyProto getProto() {
    if (builder == null) {
      builder = CatalogProtos.PartitionKeyProto.newBuilder();
    }

    if (this.columnName != null) {
      builder.setColumnName(this.columnName);
    }

    if (this.partitionValue != null) {
      builder.setPartitionValue(this.partitionValue);
    }

    return builder.build();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("name: " + partitionValue);
    return sb.toString();
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, PartitionKey.class);
  }

  public static PartitionKey fromJson(String strVal) {
    return strVal != null ? CatalogGsonHelper.fromJson(strVal, PartitionKey.class) : null;
  }

  public Object clone() throws CloneNotSupportedException {
    PartitionKey desc = (PartitionKey) super.clone();
    desc.builder = CatalogProtos.PartitionKeyProto.newBuilder();
    desc.partitionValue = partitionValue;
    desc.columnName = columnName;

    return desc;
  }

}