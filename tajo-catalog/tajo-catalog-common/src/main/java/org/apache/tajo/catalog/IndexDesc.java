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

import java.net.URI;
import java.net.URISyntaxException;

public class IndexDesc implements ProtoObject<IndexDescProto>, Cloneable {
  private String databaseName;         // required
  private String tableName;            // required
  private IndexMeta indexMeta;         // required

  public IndexDesc() {
  }
  
  public IndexDesc(String databaseName, String tableName, String indexName, URI indexPath, SortSpec[] keySortSpecs,
                   IndexMethod type,  boolean isUnique, boolean isClustered, Schema targetRelationSchema) {
    this();
    this.set(databaseName, tableName, indexName, indexPath, keySortSpecs, type, isUnique, isClustered,
        targetRelationSchema);
  }
  
  public IndexDesc(IndexDescProto proto) {
    this();

    SortSpec[] keySortSpecs = new SortSpec[proto.getKeySortSpecsCount()];
    for (int i = 0; i < keySortSpecs.length; i++) {
      keySortSpecs[i] = new SortSpec(proto.getKeySortSpecs(i));
    }

    try {
      this.set(proto.getTableIdentifier().getDatabaseName(),
          proto.getTableIdentifier().getTableName(),
          proto.getIndexName(), new URI(proto.getIndexPath()),
          keySortSpecs,
          proto.getIndexMethod(), proto.getIsUnique(), proto.getIsClustered(),
          SchemaFactory.newV1(proto.getTargetRelationSchema()));
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }

  public void set(String databaseName, String tableName, String indexName, URI indexPath, SortSpec[] keySortSpecs,
                  IndexMethod type,  boolean isUnique, boolean isClustered, Schema targetRelationSchema) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.indexMeta = new IndexMeta(indexName, indexPath, keySortSpecs, type, isUnique, isClustered,
        targetRelationSchema);
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getName() {
    return indexMeta.getIndexName();
  }
  
  public URI getIndexPath() {
    return indexMeta.getIndexPath();
  }

  public SortSpec[] getKeySortSpecs() {
    return indexMeta.getKeySortSpecs();
  }
  
  public IndexMethod getIndexMethod() {
    return indexMeta.getIndexMethod();
  }
  
  public boolean isClustered() {
    return indexMeta.isClustered();
  }
  
  public boolean isUnique() {
    return indexMeta.isUnique();
  }

  public Schema getTargetRelationSchema() {
    return indexMeta.getTargetRelationSchema();
  }

  @Override
  public IndexDescProto getProto() {
    IndexDescProto.Builder builder = IndexDescProto.newBuilder();

    CatalogProtos.TableIdentifierProto.Builder tableIdentifierBuilder = CatalogProtos.TableIdentifierProto.newBuilder();
    if (databaseName != null) {
      tableIdentifierBuilder.setDatabaseName(databaseName);
    }
    if (tableName != null) {
      tableIdentifierBuilder.setTableName(tableName);
    }

    builder.setTableIdentifier(tableIdentifierBuilder.build());
    builder.setIndexName(indexMeta.getIndexName());
    builder.setIndexPath(indexMeta.getIndexPath().toString());
    for (SortSpec colSpec : indexMeta.getKeySortSpecs()) {
      builder.addKeySortSpecs(colSpec.getProto());
    }
    builder.setIndexMethod(indexMeta.getIndexMethod());
    builder.setIsUnique(indexMeta.isUnique());
    builder.setIsClustered(indexMeta.isClustered());
    builder.setTargetRelationSchema(indexMeta.getTargetRelationSchema().getProto());

    return builder.build();
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof IndexDesc) {
      IndexDesc other = (IndexDesc) obj;
      return getDatabaseName().equals(other.getDatabaseName())
          && getTableName().equals(other.getTableName())
          && this.indexMeta.equals(other.indexMeta);
    } else {
      return false;
    }
  }
  
  public int hashCode() {
    return Objects.hashCode(databaseName, tableName, indexMeta);
  }

  public Object clone() throws CloneNotSupportedException {
    IndexDesc desc = (IndexDesc) super.clone();
    desc.databaseName = this.databaseName;
    desc.tableName = this.tableName;
    desc.indexMeta = (IndexMeta) this.indexMeta.clone();
    return desc;
  }
  
  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(this);
  }
}
