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
import org.apache.tajo.util.TUtil;

public class IndexDesc implements ProtoObject<IndexDescProto>, Cloneable {
  private IndexDescProto.Builder builder;
  
  private String indexName;            // required
  private String databaseName;         // required
  private String tableName;            // required
  private IndexMethod indexMethod;     // required
  private boolean isUnique = false;    // optional [default = false]
  private boolean isClustered = false; // optional [default = false]
  private SortSpec[] indexKeys;        // repeated
  private String predicate;            // optional
  
  public IndexDesc() {
    this.builder = IndexDescProto.newBuilder();
  }
  
  public IndexDesc(String idxName, String databaseName, String tableName, IndexMethod type,
                   boolean isUnique, boolean isClustered, SortSpec[] indexKeys, String predicate) {
    this();
    this.indexName = idxName;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.indexMethod = type;
    this.isUnique = isUnique;
    this.isClustered = isClustered;
    this.indexKeys = indexKeys;
    this.predicate = predicate;
  }
  
  public IndexDesc(IndexDescProto proto) {
    this();
    this.indexName = proto.getIndexName();
    this.databaseName = proto.getTableIdentifier().getDatabaseName();
    this.tableName = proto.getTableIdentifier().getTableName();
    this.indexMethod = proto.getMethod();
    this.isUnique = proto.getIsUnique();
    this.isClustered = proto.getIsClustered();
    this.indexKeys = new SortSpec[proto.getKeysCount()];
    for (int i = 0; i < indexKeys.length; i++) {
      this.indexKeys[i] = new SortSpec(proto.getKeys(i));
    }
    this.predicate = proto.hasPredicate() ? proto.getPredicate() : null;
  }
  
  public String getIndexName() {
    return indexName;
  }
  
  public String getTableName() {
    return tableName;
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
  
  public SortSpec[] getIndexKeys() {
    return indexKeys;
  }

  public String getPredicate() {
    return predicate;
  }

  @Override
  public IndexDescProto getProto() {
    if (builder == null) {
      builder = IndexDescProto.newBuilder();
    } else {
      builder.clear();
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
    builder.setMethod(indexMethod);
    builder.setIsUnique(this.isUnique);
    builder.setIsClustered(this.isClustered);
    for (SortSpec eachKey : indexKeys) {
      builder.addKeys(eachKey.getProto());
    }
    if (this.predicate != null) {
      builder.setPredicate(this.predicate);
    }

    return builder.build();
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof IndexDesc) {
      IndexDesc other = (IndexDesc) obj;
      return getIndexName().equals(other.getIndexName())
          && getTableName().equals(other.getTableName())
          && getIndexMethod().equals(other.getIndexMethod())
          && isUnique() == other.isUnique()
          && isClustered() == other.isClustered()
          && TUtil.checkEquals(getIndexKeys(), other.getIndexKeys())
          && TUtil.checkEquals(getPredicate(), other.getPredicate());
    } else {
      return false;
    }
  }
  
  public int hashCode() {
    return Objects.hashCode(getIndexName(), getTableName(), getIndexMethod(),
        isUnique(), isClustered(), getIndexKeys(), getPredicate());
  }

  public Object clone() throws CloneNotSupportedException {
    IndexDesc desc = (IndexDesc) super.clone();
    desc.indexName = indexName;
    desc.tableName = tableName;
    desc.indexMethod = indexMethod;
    desc.isUnique = isUnique;
    desc.isClustered = isClustered;
    desc.indexKeys = new SortSpec[this.indexKeys.length];
    for (int i = 0; i < indexKeys.length; i++) {
      desc.indexKeys[i] = new SortSpec(this.indexKeys[i].getProto());
    }
    desc.predicate = predicate;
    return desc;
  }
  
  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(this);
  }
}