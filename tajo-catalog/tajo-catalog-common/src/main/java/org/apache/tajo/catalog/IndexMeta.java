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
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexMethod;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.TUtil;

import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;

/**
 * IndexMeta contains meta information of an index.
 * Meta information is the name, an index method, a path to the stored location, index keys, and so on.
 */
public class IndexMeta implements Cloneable {
  @Expose private String indexName;              // index name
  @Expose private IndexMethod indexMethod;       // index method
  @Expose private URI indexPath;                 // path to the location
  @Expose private SortSpec[] keySortSpecs;       // index keys. This array should always be sorted
                                                 // according to the position in the targetRelationSchema
  @Expose private boolean isUnique = false;      // unique key or not
  @Expose private boolean isClustered = false;   // clustered index or not
  @Expose private Schema targetRelationSchema;   // schema of the indexed relation
  @Expose private KeyValueSet options;           // index options. TODO: will be added

  public IndexMeta() {}

  public IndexMeta(String indexName, URI indexPath, SortSpec[] keySortSpecs,
                   IndexMethod type,  boolean isUnique, boolean isClustered,
                   Schema targetRelationSchema) {
    this.indexName = indexName;
    this.indexPath = indexPath;
    this.indexMethod = type;
    this.isUnique = isUnique;
    this.isClustered = isClustered;
    this.targetRelationSchema = targetRelationSchema;
    initKeySortSpecs(targetRelationSchema, keySortSpecs);
  }

  private void initKeySortSpecs(final Schema targetRelationSchema, final SortSpec[] keySortSpecs) {
    this.targetRelationSchema = targetRelationSchema;
    this.keySortSpecs = new SortSpec[keySortSpecs.length];
    for (int i = 0; i < keySortSpecs.length; i++) {
      this.keySortSpecs[i] = new SortSpec(keySortSpecs[i].getSortKey(), keySortSpecs[i].isAscending(),
          keySortSpecs[i].isNullFirst());
    }
    Arrays.sort(this.keySortSpecs, new Comparator<SortSpec>() {
      @Override
      public int compare(SortSpec o1, SortSpec o2) {
        return targetRelationSchema.getColumnId(o1.getSortKey().getSimpleName())
            - targetRelationSchema.getColumnId(o2.getSortKey().getSimpleName());
      }
    });
  }

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(final String indexName) {
    this.indexName = indexName;
  }

  public IndexMethod getIndexMethod() {
    return indexMethod;
  }

  public void setIndexMethod(final IndexMethod type) {
    this.indexMethod = type;
  }

  public URI getIndexPath() {
    return indexPath;
  }

  public void setIndexPath(final URI indexPath) {
    this.indexPath = indexPath;
  }

  public SortSpec[] getKeySortSpecs() {
    return keySortSpecs;
  }

  public void setKeySortSpecs(final Schema targetRelationSchema, final SortSpec[] keySortSpecs) {
    initKeySortSpecs(targetRelationSchema, keySortSpecs);
  }

  public boolean isUnique() {
    return isUnique;
  }

  public void setUnique(boolean unique) {
    this.isUnique = unique;
  }

  public boolean isClustered() {
    return isClustered;
  }

  public void setClustered(boolean clustered) {
    this.isClustered = clustered;
  }

  public Schema getTargetRelationSchema() {
    return targetRelationSchema;
  }

  public KeyValueSet getOptions() {
    return options;
  }

  public void setOptions(KeyValueSet options) {
    this.options = options;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof IndexMeta) {
      IndexMeta other = (IndexMeta) o;
      return this.indexName.equals(other.indexName)
          && this.indexPath.equals(other.indexPath)
          && this.indexMethod.equals(other.indexMethod)
          && TUtil.checkEquals(this.keySortSpecs, other.keySortSpecs)
          && this.isUnique == other.isUnique
          && this.isClustered == other.isClustered
          && this.targetRelationSchema.equals(other.targetRelationSchema);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(indexName, indexPath, indexMethod, Objects.hashCode(keySortSpecs),
        isUnique, isClustered, targetRelationSchema);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    IndexMeta clone = (IndexMeta) super.clone();
    clone.indexName = indexName;
    clone.indexPath = indexPath;
    clone.indexMethod = indexMethod;
    clone.keySortSpecs = new SortSpec[keySortSpecs.length];
    for (int i = 0; i < keySortSpecs.length; i++) {
      clone.keySortSpecs[i] = new SortSpec(this.keySortSpecs[i].getProto());
    }
    clone.isUnique = this.isUnique;
    clone.isClustered = this.isClustered;
    clone.targetRelationSchema = this.targetRelationSchema;
    return clone;
  }

  @Override
  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(this);
  }
}
