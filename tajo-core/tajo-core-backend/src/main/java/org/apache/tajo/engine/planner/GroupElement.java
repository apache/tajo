/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.planner;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.tajo.algebra.Aggregation;
import org.apache.tajo.catalog.Column;

public class GroupElement implements Cloneable {
  @Expose
  private Aggregation.GroupType type;
  @Expose private Column[] columns;

  @SuppressWarnings("unused")
  public GroupElement() {
    // for gson
  }

  public GroupElement(Aggregation.GroupType type, Column[] columns) {
    this.type = type;
    this.columns = columns;
  }

  public Aggregation.GroupType getType() {
    return this.type;
  }

  public Column [] getColumns() {
    return this.columns;
  }

  public String toString() {
    Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation()
        .setPrettyPrinting().create();
    return gson.toJson(this);
  }

  public Object clone() throws CloneNotSupportedException {
    GroupElement groups = (GroupElement) super.clone();
    groups.type = type;
    groups.columns = new Column[columns.length];
    for (int i = 0; i < columns.length; i++) {
      groups.columns[i++] = (Column) columns[i].clone();
    }
    return groups;
  }
}
