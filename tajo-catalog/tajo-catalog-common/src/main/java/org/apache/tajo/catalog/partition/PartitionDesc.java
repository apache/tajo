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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionKeyProto;

import java.util.List;

/**
 * This presents each partitions of column partitioned table.
 * Each partitions can have a own name, partition path, colum name and partition value pairs.
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
 * - partitionName : dt=20150301/phone=1300/gender=m
 * - path: /tajo/data/table1/dt=20150301/phone=1300/gender=m
 * - partitionKeys:
 *    dt=20150301, phone=1300, gender=m
 *
 */
public class PartitionDesc implements ProtoObject<CatalogProtos.PartitionDescProto>, Cloneable, GsonObject {
  @Expose protected String partitionName;
  @Expose protected List<PartitionKeyProto> partitionKeys;
  @Expose protected String path; //optional

  private CatalogProtos.PartitionDescProto.Builder builder = CatalogProtos.PartitionDescProto.newBuilder();

  public String getPartitionName() {
    return partitionName;
  }

  public void setPartitionName(String partitionName) {
    this.partitionName = partitionName;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getPath() {
    return path;
  }

  public List<PartitionKeyProto> getPartitionKeys() {
    return partitionKeys;
  }

  public void setPartitionKeys(List<PartitionKeyProto> partitionKeys) {
    this.partitionKeys = partitionKeys;
  }

  public int hashCode() {
    return Objects.hashCode(partitionName, partitionKeys, path);
  }

  public boolean equals(Object o) {
    if (o instanceof PartitionDesc) {
      PartitionDesc another = (PartitionDesc) o;
      boolean eq = ((partitionName != null && another.partitionName != null
          && partitionName.equals(another.partitionName)) ||
          (partitionName == null && another.partitionName == null));
      eq = eq && ((partitionKeys != null && another.partitionKeys != null
                     && partitionKeys.equals(another.partitionKeys))
                 || (partitionKeys == null && another.partitionKeys == null));
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
      builder.setPartitionName(this.partitionName);
    }

    builder.clearPartitionKeys();
    if (this.partitionKeys != null) {
      for(PartitionKeyProto partitionKey : this.partitionKeys) {
        builder.addPartitionKeys(partitionKey);
      }
    }

    if(this.path != null) {
      builder.setPath(this.path);
    }

    return builder.build();
  }

  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().
      excludeFieldsWithoutExposeAnnotation().create();
    return gson.toJson(this);
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
    desc.partitionKeys = partitionKeys;
    desc.path = path;

    return desc;
  }

}