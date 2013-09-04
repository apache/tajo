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

import org.apache.tajo.util.TUtil;

import java.util.Map;

public class Insert extends Expr {
  private boolean overwrite = false;
  private String tableName;
  private String [] targetColumns;
  private String storageType;
  private String location;
  private Expr subquery;
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
  boolean equalsTo(Expr expr) {
    Insert another = (Insert) expr;
    return overwrite == another.overwrite &&
        TUtil.checkEquals(tableName, another.tableName) &&
        TUtil.checkEquals(subquery, another.subquery) &&
        TUtil.checkEquals(storageType, another.storageType) &&
        TUtil.checkEquals(location, another.location) &&
        TUtil.checkEquals(params, another.params);
  }
}
