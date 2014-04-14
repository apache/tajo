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

package org.apache.tajo.catalog;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;
import org.apache.tajo.common.ProtoObject;

public class IndexDesc implements ProtoObject<IndexDescProto>, Cloneable {
  private IndexDescProto.Builder builder;
  
  private String indexName;            // required
  private String databaseName;         // required
  private String tableName;            // required
  private Column column;               // required
  private IndexMethod indexMethod;     // required
  private boolean isUnique = false;    // optional [default = false]
  private boolean isClustered = false; // optional [default = false]
  private boolean isAscending = false; // optional [default = false]
  
  public IndexDesc() {
    this.builder = IndexDescProto.newBuilder();
  }
  
  public IndexDesc(String idxName, String databaseName, String tableName, Column column,
                   IndexMethod type,  boolean isUnique, boolean isClustered, boolean isAscending) {
    this();
    this.indexName = idxName;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.column = column;
    this.indexMethod = type;
    this.isUnique = isUnique;
    this.isClustered = isClustered;
    this.isAscending = isAscending;
  }
  
  public IndexDesc(IndexDescProto proto) {
    this(proto.getIndexName(),
        proto.getTableIdentifier().getDatabaseName(),
        proto.getTableIdentifier().getTableName(),
        new Column(proto.getColumn()),
        proto.getIndexMethod(), proto.getIsUnique(), proto.getIsClustered(), proto.getIsAscending());
  }
  
  public String getIndexName() {
    return indexName;
  }
  
  public String getTableName() {
    return tableName;
  }
  
  public Column getColumn() {
    return column;
  }
  
  public IndexMethod getIndexMethod() {
    return this.indexMethod;
  }
  
  public boolean isClustered() {
    return this.isClustered;
  }
  
  public boolean isUnique() {
    return this.isUnique;
  }
  
  public boolean isAscending() {
    return this.isAscending;
  }

  @Override
  public IndexDescProto getProto() {
    if (builder == null) {
      builder = IndexDescProto.newBuilder();
    }

    CatalogProtos.TableIdentifierProto.Builder tableIdentifierBuilder = CatalogProtos.TableIdentifierProto.newBuilder();
    if (databaseName != null) {
      tableIdentifierBuilder.setDatabaseName(databaseName);
    }
    if (tableName != null) {
      tableIdentifierBuilder.setTableName(tableName);
    }

    builder.setTableIdentifier(tableIdentifierBuilder.build());
    builder.setIndexName(this.indexName);
    builder.setColumn(this.column.getProto());
    builder.setIndexMethod(indexMethod);
    builder.setIsUnique(this.isUnique);
    builder.setIsClustered(this.isClustered);
    builder.setIsAscending(this.isAscending);

    return builder.build();
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof IndexDesc) {
      IndexDesc other = (IndexDesc) obj;
      return getIndexName().equals(other.getIndexName())
          && getTableName().equals(other.getTableName())
          && getColumn().equals(other.getColumn())
          && getIndexMethod().equals(other.getIndexMethod())
          && isUnique() == other.isUnique()
          && isClustered() == other.isClustered()
          && isAscending() == other.isAscending();
    } else {
      return false;
    }
  }
  
  public int hashCode() {
    return Objects.hashCode(getIndexName(), getTableName(), getColumn(),
        getIndexMethod(), isUnique(), isClustered(), isAscending());
  }

  public Object clone() throws CloneNotSupportedException {
    IndexDesc desc = (IndexDesc) super.clone();
    desc.indexName = indexName;
    desc.tableName = tableName;
    desc.column = column;
    desc.indexMethod = indexMethod;
    desc.isUnique = isUnique;
    desc.isClustered = isClustered;
    desc.isAscending = isAscending;
    return desc;
  }
  
  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(this);
  }
}