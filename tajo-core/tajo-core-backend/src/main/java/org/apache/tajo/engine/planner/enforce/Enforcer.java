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

package org.apache.tajo.engine.planner.enforce;


import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.util.TUtil;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.tajo.ipc.TajoWorkerProtocol.*;
import static org.apache.tajo.ipc.TajoWorkerProtocol.EnforceProperty.EnforceType;
import static org.apache.tajo.ipc.TajoWorkerProtocol.GroupbyEnforce.GroupbyAlgorithm;

public class Enforcer implements ProtoObject<EnforcerProto> {
  Map<EnforceType, List<EnforceProperty>> properties;
  private EnforcerProto proto;

  @SuppressWarnings("unused")
  public Enforcer() {
    properties = TUtil.newHashMap();
  }

  public Enforcer(EnforcerProto proto) {
    this.proto = proto;
  }

  private EnforceProperty.Builder newProperty() {
    return EnforceProperty.newBuilder();
  }

  private void initProperties() {
    if (properties == null) {
      properties = TUtil.newHashMap();
      for (EnforceProperty property : proto.getPropertiesList()) {
        TUtil.putToNestedList(properties, property.getType(), property);
      }
    }
  }

  public boolean hasEnforceProperty(EnforceType type) {
    initProperties();
    return properties.containsKey(type);
  }

  public List<EnforceProperty> getEnforceProperties(EnforceType type) {
    initProperties();
    return properties.get(type);
  }

  public void addSortedInput(String tableName, SortSpec[] sortSpecs) {
    EnforceProperty.Builder builder = newProperty();
    SortedInputEnforce.Builder enforce = SortedInputEnforce.newBuilder();
    enforce.setTableName(tableName);
    for (SortSpec sortSpec : sortSpecs) {
      enforce.addSortSpecs(sortSpec.getProto());
    }

    builder.setType(EnforceType.SORTED_INPUT);
    builder.setSortedInput(enforce.build());
    TUtil.putToNestedList(properties, builder.getType(), builder.build());
  }

  public void addOutputDistinct() {
    EnforceProperty.Builder builder = newProperty();
    OutputDistinctEnforce.Builder enforce = OutputDistinctEnforce.newBuilder();

    builder.setType(EnforceType.OUTPUT_DISTINCT);
    builder.setOutputDistinct(enforce.build());
    TUtil.putToNestedList(properties, builder.getType(), builder.build());
  }

  public void enforceJoinAlgorithm(int pid, JoinEnforce.JoinAlgorithm algorithm) {
    EnforceProperty.Builder builder = newProperty();
    JoinEnforce.Builder enforce = JoinEnforce.newBuilder();
    enforce.setPid(pid);
    enforce.setAlgorithm(algorithm);

    builder.setType(EnforceType.JOIN);
    builder.setJoin(enforce.build());
    TUtil.putToNestedList(properties, builder.getType(), builder.build());
  }

  public void enforceSortAggregation(int pid, @Nullable SortSpec[] sortSpecs) {
    EnforceProperty.Builder builder = newProperty();
    GroupbyEnforce.Builder enforce = GroupbyEnforce.newBuilder();
    enforce.setPid(pid);
    enforce.setAlgorithm(GroupbyAlgorithm.SORT_AGGREGATION);
    if (sortSpecs != null) {
      for (SortSpec sortSpec : sortSpecs) {
        enforce.addSortSpecs(sortSpec.getProto());
      }
    }

    builder.setType(EnforceType.GROUP_BY);
    builder.setGroupby(enforce.build());
    TUtil.putToNestedList(properties, builder.getType(), builder.build());
  }

  public void enforceHashAggregation(int pid) {
    EnforceProperty.Builder builder = newProperty();
    GroupbyEnforce.Builder enforce = GroupbyEnforce.newBuilder();
    enforce.setPid(pid);
    enforce.setAlgorithm(GroupbyAlgorithm.HASH_AGGREGATION);

    builder.setType(EnforceType.GROUP_BY);
    builder.setGroupby(enforce.build());
    TUtil.putToNestedList(properties, builder.getType(), builder.build());
  }

  public void enforceSortAlgorithm(int pid, SortEnforce.SortAlgorithm algorithm) {
    EnforceProperty.Builder builder = newProperty();
    SortEnforce.Builder enforce = SortEnforce.newBuilder();
    enforce.setPid(pid);
    enforce.setAlgorithm(algorithm);

    builder.setType(EnforceType.SORT);
    builder.setSort(enforce.build());
    TUtil.putToNestedList(properties, builder.getType(), builder.build());
  }

  public void addBroadcast(String tableName) {
    EnforceProperty.Builder builder = newProperty();
    BroadcastEnforce.Builder enforce = BroadcastEnforce.newBuilder();
    enforce.setTableName(tableName);

    builder.setType(EnforceType.BROADCAST);
    builder.setBroadcast(enforce);
    TUtil.putToNestedList(properties, builder.getType(), builder.build());
  }

  public Collection<EnforceProperty> getProperties() {
    if (proto != null) {
      return proto.getPropertiesList();
    } else {
      List<EnforceProperty> list = TUtil.newList();
      for (List<EnforceProperty> propertyList : properties.values()) {
        list.addAll(propertyList);
      }
      return list;
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("Enforce ").append(properties.size()).append(" properties: ");
    boolean first = true;
    for (EnforceType enforceType : properties.keySet()) {
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      sb.append(enforceType);
    }
    return sb.toString();
  }

  @Override
  public EnforcerProto getProto() {
    EnforcerProto.Builder builder = EnforcerProto.newBuilder();
    builder.addAllProperties(getProperties());
    return builder.build();
  }
}
