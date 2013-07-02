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

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.json.GsonCreator;

public class SortSpec implements Cloneable {
  @Expose
  private Column sortKey;
  @Expose private boolean ascending = true;
  @Expose private boolean nullFirst = false;

  public SortSpec(final Column sortKey) {
    this.sortKey = sortKey;
  }

  /**
   *
   * @param sortKey columns to sort
   * @param asc true if the sort order is ascending order
   * @param nullFirst
   * Otherwise, it should be false.
   */
  public SortSpec(final Column sortKey, final boolean asc,
                  final boolean nullFirst) {
    this(sortKey);
    this.ascending = asc;
    this.nullFirst = nullFirst;
  }

  public final boolean isAscending() {
    return this.ascending;
  }

  public final void setDescOrder() {
    this.ascending = false;
  }

  public final boolean isNullFirst() {
    return this.nullFirst;
  }

  public final void setNullFirst() {
    this.nullFirst = true;
  }

  public final Column getSortKey() {
    return this.sortKey;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    SortSpec key = (SortSpec) super.clone();
    key.sortKey = (Column) sortKey.clone();
    key.ascending = ascending;

    return key;
  }

  public String toJSON() {
    sortKey.initFromProto();
    return GsonCreator.getInstance().toJson(this);
  }

  public String toString() {
    return "Sortkey (key="+sortKey
        + " "+(ascending ? "asc" : "desc")+")";
  }
}
