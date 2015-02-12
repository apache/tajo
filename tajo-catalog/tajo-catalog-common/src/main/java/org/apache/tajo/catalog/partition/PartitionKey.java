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

/**
 * <code>PartitionDesc</code> presents a table partition.
 */
public class PartitionKey implements ProtoObject<CatalogProtos.PartitionKeyProto>, Cloneable, GsonObject {
  @Expose protected String partitionValue;                      // required
  @Expose protected int idx;                       // required

  private CatalogProtos.PartitionKeyProto.Builder builder = CatalogProtos.PartitionKeyProto.newBuilder();

  public PartitionKey() {
  }

  public PartitionKey(String partitionValue, int dx) {
    this.partitionValue = partitionValue;
    this.idx = idx;
  }

  public PartitionKey(PartitionKey partition) {
    this.partitionValue = partition.partitionValue;
    this.idx = partition.idx;
  }

  public PartitionKey(CatalogProtos.PartitionKeyProto proto) {
    if (proto.hasIdx()) {
      this.partitionValue = proto.getPartitionValue();
    }

    this.idx = proto.getIdx();
  }

  public String getPartitionValue() {
    return partitionValue;
  }

  public void setPartitionValue(String partitionValue) {
    this.partitionValue = partitionValue;
  }

  public int getIdx() {
    return idx;
  }

  public void setIdx(int idx) {
    this.idx = idx;
  }

  public int hashCode() {
    return Objects.hashCode(partitionValue, idx);
  }

  public boolean equals(Object o) {
    if (o instanceof PartitionKey) {
      PartitionKey another = (PartitionKey) o;
      boolean eq = ((partitionValue != null && another.partitionValue != null
          && partitionValue.equals(another.partitionValue)) ||
          (partitionValue == null && another.partitionValue == null));
      eq = eq && (idx == another.idx);
      return eq;
    }
    return false;
  }


  @Override
  public CatalogProtos.PartitionKeyProto getProto() {
    if (builder == null) {
      builder = CatalogProtos.PartitionKeyProto.newBuilder();
    }

    if(this.partitionValue != null) {
      builder.setPartitionValue(this.partitionValue);
    }

    builder.setIdx(this.idx);

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
    desc.idx = idx;

    return desc;
  }

}