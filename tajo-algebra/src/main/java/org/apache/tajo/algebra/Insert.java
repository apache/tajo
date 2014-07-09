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

import java.util.HashMap;
import java.util.Map;

public class Insert extends Expr {
  @Expose @SerializedName("IsOverwrite")
  private boolean overwrite = false;
  @Expose @SerializedName("TableName")
  private String tableName;
  @Expose @SerializedName("TargetColumns")
  private String [] targetColumns;
  @Expose @SerializedName("StorageType")
  private String storageType;
  @Expose @SerializedName("Location")
  private String location;
  @Expose @SerializedName("SubPlan")
  private Expr subquery;
  @Expose @SerializedName("InsertParams")
  private Map<String, String> params;

  public Insert() {
    super(OpType.Insert);
  }

  public void setOverwrite() {
    overwrite = true;
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  public boolean hasTableName() {
    return this.tableName != null;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return this.tableName;
  }

  public boolean hasTargetColumns() {
    return targetColumns != null;
  }

  public String [] getTargetColumns() {
    return targetColumns;
  }

  public void setTargetColumns(String [] targets) {
    this.targetColumns = targets;
  }

  public boolean hasLocation() {
    return location != null;
  }

  public String getLocation() {
    return this.location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public boolean hasStorageType() {
    return storageType != null;
  }

  public void setStorageType(String storageType) {
    this.storageType = storageType;
  }

  public String getStorageType() {
    return storageType;
  }

  public boolean hasParams() {
    return params != null;
  }

  public Map<String, String> getParams() {
    return params;
  }

  public void setParams(Map<String, String> params) {
    this.params = params;
  }

  public void setSubQuery(Expr subquery) {
    this.subquery = subquery;
  }

  public Expr getSubQuery() {
    return subquery;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        overwrite, tableName, Objects.hashCode(targetColumns),
        subquery, storageType, location,
        params);
  }

  @Override
  boolean equalsTo(Expr expr) {
    Insert another = (Insert) expr;
    return overwrite == another.overwrite &&
        TUtil.checkEquals(tableName, another.tableName) &&
        TUtil.checkEquals(targetColumns, another.targetColumns) &&
        TUtil.checkEquals(subquery, another.subquery) &&
        TUtil.checkEquals(storageType, another.storageType) &&
        TUtil.checkEquals(location, another.location) &&
        TUtil.checkEquals(params, another.params);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    Insert insert = (Insert) super.clone();
    insert.overwrite = overwrite;
    insert.tableName = tableName;
    insert.targetColumns = targetColumns != null ? targetColumns.clone() : null;
    insert.storageType = storageType;
    insert.location = location;
    insert.subquery = (Expr) subquery.clone();
    insert.params = new HashMap<String, String>(params);
    return insert;
  }
}
