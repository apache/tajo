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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaFactory;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TUtil;

import static org.apache.tajo.catalog.proto.CatalogProtos.PartitionType;
import static org.apache.tajo.catalog.proto.CatalogProtos.TableIdentifierProto;

/**
 * <code>PartitionMethodDesc</code> presents a table description, including partition type, and partition keys.
 */
public class PartitionMethodDesc implements ProtoObject<CatalogProtos.PartitionMethodProto>, Cloneable, GsonObject {
  @Expose private String databaseName;                         // required
  @Expose private String tableName;                            // required
  @Expose private PartitionType partitionType;                 // required
  @Expose private String expression;                           // required
  @Expose private Schema expressionSchema;                     // required

  public PartitionMethodDesc() {
  }

  public PartitionMethodDesc(String databaseName, String tableName,
                             PartitionType partitionType, String expression,
                             Schema expressionSchema) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionType = partitionType;
    this.expression = expression;
    this.expressionSchema = expressionSchema;
  }

  public PartitionMethodDesc(CatalogProtos.PartitionMethodProto proto) {
    this(proto.getTableIdentifier().getDatabaseName(),
        proto.getTableIdentifier().getTableName(),
        proto.getPartitionType(), proto.getExpression(),
        SchemaFactory.newV1(proto.getExpressionSchema()));
  }

  public String getTableName() {
    return tableName;
  }

  public String getExpression() {
    return expression;
  }

  public Schema getExpressionSchema() {
    return expressionSchema;
  }

  public PartitionType getPartitionType() {
    return partitionType;
  }

  public void setTableName(String tableId) {
    this.tableName = tableId;
  }

  public void setExpressionSchema(Schema expressionSchema) {
    this.expressionSchema = expressionSchema;
  }

  public void setPartitionType(PartitionType partitionsType) {
    this.partitionType = partitionsType;
  }

  public void setExpression(String expression) {
    this.expression = expression;
  }

  @Override
  public boolean equals(Object object) {
    if(object instanceof PartitionMethodDesc) {
      PartitionMethodDesc other = (PartitionMethodDesc) object;
      boolean eq = tableName.equals(other.tableName);
      eq = eq && partitionType.equals(other.partitionType);
      eq = eq && expression.equals(other.expression);
      eq = eq && TUtil.checkEquals(expressionSchema, other.expressionSchema);
      return eq;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName, partitionType, expression, expressionSchema);
  }

  @Override
  public CatalogProtos.PartitionMethodProto getProto() {
    TableIdentifierProto.Builder tableIdentifierBuilder = TableIdentifierProto.newBuilder();
    if (databaseName != null) {
      tableIdentifierBuilder.setDatabaseName(databaseName);
    }
    if (tableName != null) {
      tableIdentifierBuilder.setTableName(tableName);
    }

    CatalogProtos.PartitionMethodProto.Builder builder = CatalogProtos.PartitionMethodProto.newBuilder();
    builder.setTableIdentifier(tableIdentifierBuilder.build());
    builder.setPartitionType(partitionType);
    builder.setExpression(expression);
    builder.setExpressionSchema(expressionSchema.getProto());
    return builder.build();
  }


  public Object clone() throws CloneNotSupportedException {
    PartitionMethodDesc desc = (PartitionMethodDesc) super.clone();
    desc.tableName = tableName;
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