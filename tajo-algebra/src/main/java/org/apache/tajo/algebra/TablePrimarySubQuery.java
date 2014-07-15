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

package org.apache.tajo.algebra;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.tajo.util.TUtil;

public class TablePrimarySubQuery extends Relation {
  @Expose @SerializedName("SubPlan")
  private Expr subquery;
  @Expose @SerializedName("ColumnNames")
  private String [] columnNames;

  public TablePrimarySubQuery(String relName, Expr subquery) {
    super(OpType.TablePrimaryTableSubQuery, relName);
    this.subquery = subquery;
  }

  public boolean hasColumnNames() {
    return this.columnNames != null;
  }

  public void setColumnNames(String[] aliasList) {
    this.columnNames = aliasList;
  }

  public String [] getColumnNames() {
    return columnNames;
  }

  public Expr getSubQuery() {
    return subquery;
  }

  public int hashCode() {
    return Objects.hashCode(subquery, Objects.hashCode(columnNames));
  }

  @Override
  boolean equalsTo(Expr expr) {
    TablePrimarySubQuery another = (TablePrimarySubQuery) expr;
    return subquery.equals(another.subquery) && TUtil.checkEquals(columnNames, another.columnNames);
  }

  public String toJson() {
    return JsonHelper.toJson(this);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    TablePrimarySubQuery subQuery = (TablePrimarySubQuery) super.clone();
    subQuery.subquery = (Expr) subquery.clone();
    if (columnNames != null) {
      subQuery.columnNames = columnNames.clone();
    }
    return subQuery;
  }
}
