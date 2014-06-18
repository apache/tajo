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
import org.apache.tajo.catalog.proto.CatalogProtos.IndexKeyProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.util.TUtil;

import java.util.List;

public class IndexDesc implements ProtoObject<IndexDescProto>, Cloneable {
  private IndexDescProto.Builder builder;
  
  private String indexName;            // required
  private String databaseName;         // required
  private String tableName;            // required
  private IndexMethod indexMethod;     // required
  private List<IndexKey> keys;
  private boolean isUnique = false;    // optional [default = false]
  private boolean isClustered = false; // optional [default = false]
  private String predicate;            // optional
  
  public IndexDesc() {
    this.builder = IndexDescProto.newBuilder();
  }
  
  public IndexDesc(String idxName, String databaseName, String tableName, IndexMethod type,
                   List<IndexKey> keys, boolean isUnique, boolean isClustered, String predicate) {
    this();
    this.indexName = idxName;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.indexMethod = type;
    this.keys = keys;
    this.isUnique = isUnique;
    this.isClustered = isClustered;
    this.predicate = predicate;
  }
  
  public IndexDesc(IndexDescProto proto) {
    this();
    this.indexName = proto.getIndexName();
    this.databaseName = proto.getTableIdentifier().getDatabaseName();
    this.tableName = proto.getTableIdentifier().getTableName();
    this.indexMethod = proto.getMethod();
    this.keys = TUtil.newList();
    for (IndexKeyProto eachProto : proto.getKeysList()) {
      this.keys.add(new IndexKey(eachProto));
    }
    this.isUnique = proto.getIsUnique();
    this.isClustered = proto.getIsClustered();
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
  
  public List<IndexKey> getKeys() {
    return keys;
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
    for (IndexKey eachKey : keys) {
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
          && TUtil.checkEquals(getKeys(), other.getKeys())
          && TUtil.checkEquals(getPredicate(), other.getPredicate());
    } else {
      return false;
    }
  }
  
  public int hashCode() {
    return Objects.hashCode(getIndexName(), getTableName(), getIndexMethod(),
        isUnique(), isClustered(), getKeys(), getPredicate());
  }

  public Object clone() throws CloneNotSupportedException {
    IndexDesc desc = (IndexDesc) super.clone();
    desc.indexName = indexName;
    desc.tableName = tableName;
    desc.indexMethod = indexMethod;
    desc.isUnique = isUnique;
    desc.isClustered = isClustered;
    desc.keys = TUtil.newList();
    for (IndexKey eachKey : keys) {
      desc.keys.add((IndexKey) eachKey.clone());
    }
    desc.predicate = predicate;
    return desc;
  }
  
  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(this);
  }

  public static class IndexKey implements ProtoObject<IndexKeyProto>, Cloneable {
    private final IndexKeyProto.Builder builder;
    private String keyJson;
    private boolean ascending = true;
    private boolean nullFirst = false;

    public IndexKey() {
      builder = IndexKeyProto.newBuilder();
    }

    public IndexKey(final String keyJson, final boolean ascending, final boolean nullFirst) {
      this();
      this.keyJson = keyJson;
      this.ascending = ascending;
      this.nullFirst = nullFirst;
    }

    public IndexKey(IndexKeyProto proto) {
      this(proto.getKeyJson(), proto.getAscending(), proto.getNullFirst());
    }

    @Override
    public IndexKeyProto getProto() {
      if (builder != null) {
        builder.clear();
      }
      builder.setKeyJson(keyJson);
      builder.setAscending(ascending);
      builder.setNullFirst(nullFirst);
      return builder.build();
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
      IndexKey clone = (IndexKey) super.clone();
      clone.keyJson = this.keyJson;
      clone.ascending = this.ascending;
      clone.nullFirst = this.nullFirst;
      return clone;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof IndexKey) {
        IndexKey other = (IndexKey) o;
        return this.keyJson.equals(other.keyJson) &&
            this.ascending == other.ascending &&
            this.nullFirst == other.nullFirst;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(keyJson, ascending, nullFirst);
    }

    public String getKeyJson() {
      return keyJson;
    }

    public boolean isAscending() {
      return ascending;
    }

    public boolean isNullFirst() {
      return nullFirst;
    }
  }
}