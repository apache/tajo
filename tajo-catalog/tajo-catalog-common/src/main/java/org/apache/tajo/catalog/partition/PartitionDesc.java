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
public class PartitionDesc implements ProtoObject<CatalogProtos.PartitionDescProto>, Cloneable, GsonObject {
  @Expose protected String partitionName;                      // optional
  @Expose protected int ordinalPosition;                       // required
  @Expose protected String partitionValue;                     // optional
  @Expose protected String path;                               // optional

  private CatalogProtos.PartitionDescProto.Builder builder = CatalogProtos.PartitionDescProto.newBuilder();

  public PartitionDesc() {
  }

  public PartitionDesc(PartitionDesc partition) {
    this.partitionName = partition.partitionName;
    this.ordinalPosition = partition.ordinalPosition;
    this.partitionValue = partition.partitionValue;
    this.path = partition.path;
  }

  public PartitionDesc(CatalogProtos.PartitionDescProto proto) {
    if(proto.hasPartitionName()) {
      this.partitionName = proto.getPartitionName();
    }
    this.ordinalPosition = proto.getOrdinalPosition();
    if(proto.hasPartitionValue()) {
      this.partitionValue = proto.getPartitionValue();
    }
    if(proto.hasPath()) {
      this.path = proto.getPath();
    }
  }

  public void setName(String partitionName) {
    this.partitionName = partitionName;
  }
  public String getName() {
    return partitionName;
  }


  public void setOrdinalPosition(int ordinalPosition) {
    this.ordinalPosition = ordinalPosition;
  }
  public int getOrdinalPosition() {
    return ordinalPosition;
  }

  public void setPartitionValue(String partitionValue) {
    this.partitionValue = partitionValue;
  }
  public String getPartitionValue() {
    return partitionValue;
  }

  public void setPath(String path) {
    this.path = path;
  }
  public String getPath() {
    return path;
  }

  public int hashCode() {
    return Objects.hashCode(partitionName, ordinalPosition, partitionValue, path);
  }

  public boolean equals(Object o) {
    if (o instanceof PartitionDesc) {
      PartitionDesc another = (PartitionDesc) o;
      boolean eq = ((partitionName != null && another.partitionName != null
          && partitionName.equals(another.partitionName)) ||
          (partitionName == null && another.partitionName == null));
      eq = eq && (ordinalPosition == another.ordinalPosition);
      eq = eq && ((partitionValue != null && another.partitionValue != null
                     && partitionValue.equals(another.partitionValue))
                 || (partitionValue == null && another.partitionValue == null));
      eq = eq && ((path != null && another.path != null && path.equals(another.path)) ||
          (path == null && another.path == null));
      return eq;
    }
    return false;
  }


  @Override
  public CatalogProtos.PartitionDescProto getProto() {
    if (builder == null) {
      builder = CatalogProtos.PartitionDescProto.newBuilder();
    }

    if(this.partitionName != null) {
      builder.setPartitionName(partitionName);
    }

    builder.setOrdinalPosition(this.ordinalPosition);

    if (this.partitionValue != null) {
      builder.setPartitionValue(this.partitionValue);
    }

    if(this.path != null) {
      builder.setPath(this.path);
    }

    return builder.build();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("name: " + partitionName);
    return sb.toString();
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, PartitionDesc.class);
  }

  public static PartitionDesc fromJson(String strVal) {
    return strVal != null ? CatalogGsonHelper.fromJson(strVal, PartitionDesc.class) : null;
  }

  public Object clone() throws CloneNotSupportedException {
    PartitionDesc desc = (PartitionDesc) super.clone();
    desc.builder = CatalogProtos.PartitionDescProto.newBuilder();
    desc.partitionName = partitionName;
    desc.ordinalPosition = ordinalPosition;
    desc.partitionValue = partitionValue;
    desc.path = path;

    return desc;
  }

}