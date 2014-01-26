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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TUtil;

/**
 * <code>PartitionMethodDesc</code> presents a table description, including partition type, and partition keys.
 */
public class PartitionMethodDesc implements ProtoObject<CatalogProtos.PartitionMethodProto>, Cloneable, GsonObject {
  private CatalogProtos.PartitionMethodProto.Builder builder;

  @Expose private String tableId;                                       // required
  @Expose private CatalogProtos.PartitionType partitionType;            // required
  @Expose private String expression;                                    // required
  @Expose private Schema expressionSchema;                              // required

  public PartitionMethodDesc() {
    builder = CatalogProtos.PartitionMethodProto.newBuilder();
  }

  public PartitionMethodDesc(String tableId, CatalogProtos.PartitionType partitionType, String expression,
                             Schema expressionSchema) {
    this.tableId = tableId;
    this.partitionType = partitionType;
    this.expression = expression;
    this.expressionSchema = expressionSchema;
  }

  public PartitionMethodDesc(CatalogProtos.PartitionMethodProto proto) {
    this(proto.getTableId(), proto.getPartitionType(), proto.getExpression(), new Schema(proto.getExpressionSchema()));
  }

  public String getTableId() {
    return tableId;
  }

  public String getExpression() {
    return expression;
  }

  public Schema getExpressionSchema() {
    return expressionSchema;
  }

  public CatalogProtos.PartitionType getPartitionType() {
    return partitionType;
  }

  public void setTableId(String tableId) {
    this.tableId = tableId;
  }

  public void setExpressionSchema(Schema expressionSchema) {
    this.expressionSchema = expressionSchema;
  }

  public void setPartitionType(CatalogProtos.PartitionType partitionsType) {
    this.partitionType = partitionsType;
  }

  public void setExpression(String expression) {
    this.expression = expression;
  }

  @Override
  public boolean equals(Object object) {
    if(object instanceof PartitionMethodDesc) {
      PartitionMethodDesc other = (PartitionMethodDesc) object;
      boolean eq = tableId.equals(other.tableId);
      eq = eq && partitionType.equals(other.partitionType);
      eq = eq && expression.equals(other.expression);
      eq = eq && TUtil.checkEquals(expressionSchema, other.expressionSchema);
      return eq;
    }

    return false;
  }

  @Override
  public CatalogProtos.PartitionMethodProto getProto() {
    if(builder == null) {
      builder = CatalogProtos.PartitionMethodProto.newBuilder();
    }
    builder.setTableId(tableId);
    builder.setPartitionType(partitionType);
    builder.setExpression(expression);
    builder.setExpressionSchema(expressionSchema.getProto());
    return builder.build();
  }


  public Object clone() throws CloneNotSupportedException {
    PartitionMethodDesc desc = (PartitionMethodDesc) super.clone();
    desc.builder = builder;
    desc.tableId = tableId;
    desc.partitionType = partitionType;
    desc.expression = expression;
    desc.expressionSchema = (Schema) expressionSchema.clone();
    return desc;
  }

  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().
        excludeFieldsWithoutExposeAnnotation().create();
    return gson.toJson(this);
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, PartitionMethodDesc.class);
  }

  public static PartitionMethodDesc fromJson(String strVal) {
    return strVal != null ? CatalogGsonHelper.fromJson(strVal, PartitionMethodDesc.class) : null;
  }
}