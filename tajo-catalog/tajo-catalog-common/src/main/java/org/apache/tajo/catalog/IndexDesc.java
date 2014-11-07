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
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;
import org.apache.tajo.common.ProtoObject;

public class IndexDesc implements ProtoObject<IndexDescProto>, Cloneable {
  private String databaseName;         // required
  private String tableName;            // required
  private String name;                 // required
  private IndexMethod indexMethod;     // required
  private Path indexPath;              // required
  private SortSpec[] keySortSpecs;  // required
  private boolean isUnique = false;    // optional [default = false]
  private boolean isClustered = false; // optional [default = false]

  public IndexDesc() {
  }
  
  public IndexDesc(String name, Path indexPath, String databaseName, String tableName, SortSpec[] keySortSpecs,
                   IndexMethod type,  boolean isUnique, boolean isClustered) {
    this();
    this.set(name, indexPath, databaseName, tableName, keySortSpecs, type, isUnique, isClustered);
  }
  
  public IndexDesc(IndexDescProto proto) {
    this();

    SortSpec[] keySortSpecs = new SortSpec[proto.getColumnSpecsCount()];
    for (int i = 0; i < keySortSpecs.length; i++) {
      keySortSpecs[i] = new SortSpec(proto.getColumnSpecs(i));
    }

    this.set(proto.getName(), new Path(proto.getIndexPath()),
        proto.getTableIdentifier().getDatabaseName(),
        proto.getTableIdentifier().getTableName(),
        keySortSpecs,
        proto.getIndexMethod(), proto.getIsUnique(), proto.getIsClustered());
  }

  public void set(String name, Path indexPath, String databaseName, String tableName, SortSpec[] keySortSpecs,
                  IndexMethod type,  boolean isUnique, boolean isClustered) {
    this.name = name;
    this.indexPath = indexPath;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.indexMethod = type;
    this.keySortSpecs = keySortSpecs;
    this.isUnique = isUnique;
    this.isClustered = isClustered;
  }

  public String getName() {
    return name;
  }
  
  public Path getIndexPath() {
    return indexPath;
  }
  
  public String getTableName() {
    return tableName;
  }
  
  public SortSpec[] getKeySortSpecs() {
    return keySortSpecs;
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
    builder.setName(this.name);
    builder.setIndexPath(this.indexPath.toString());
    for (SortSpec colSpec : keySortSpecs) {
      builder.addColumnSpecs(colSpec.getProto());
    }
    builder.setIndexMethod(indexMethod);
    builder.setIsUnique(this.isUnique);
    builder.setIsClustered(this.isClustered);

    return builder.build();
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof IndexDesc) {
      IndexDesc other = (IndexDesc) obj;
      return getIndexPath().equals(other.getIndexPath())
          && getName().equals(other.getName())
          && getTableName().equals(other.getTableName())
          && getKeySortSpecs().equals(other.getKeySortSpecs())
          && getIndexMethod().equals(other.getIndexMethod())
          && isUnique() == other.isUnique()
          && isClustered() == other.isClustered();
    } else {
      return false;
    }
  }
  
  public int hashCode() {
    return Objects.hashCode(getName(), getIndexPath(), getTableName(), getKeySortSpecs(),
        getIndexMethod(), isUnique(), isClustered());
  }

  public Object clone() throws CloneNotSupportedException {
    IndexDesc desc = (IndexDesc) super.clone();
    desc.name = name;
    desc.indexPath = indexPath;
    desc.tableName = tableName;
    desc.keySortSpecs = keySortSpecs;
    desc.indexMethod = indexMethod;
    desc.isUnique = isUnique;
    desc.isClustered = isClustered;
    return desc;
  }
  
  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(this);
  }
}
