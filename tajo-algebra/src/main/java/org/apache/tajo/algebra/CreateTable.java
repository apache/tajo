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

public class CreateTable extends Expr {
  private boolean external = false;
  private String tableName;
  private ColumnDefinition [] tableElements;
  private String storageType;
  private String location;
  private Expr subquery;
  private Map<String, String> params;

  public CreateTable(final String tableName) {
    super(OpType.CreateTable);
    this.tableName = tableName;
  }

  public CreateTable(final String tableName, final Expr subQuery) {
    this(tableName);
    this.subquery = subQuery;
  }

  public void setExternal() {
    external = true;
  }

  public boolean isExternal() {
    return external;
  }

  public String getTableName() {
    return this.tableName;
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

  public boolean hasTableElements() {
    return this.tableElements != null;
  }

  public ColumnDefinition [] getTableElements() {
    return tableElements;
  }

  public void setTableElements(ColumnDefinition [] tableElements) {
    this.tableElements = tableElements;
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

  public boolean hasSubQuery() {
    return subquery != null;
  }

  public void setSubQuery(Expr subquery) {
    this.subquery = subquery;
  }

  public Expr getSubQuery() {
    return subquery;
  }

  @Override
  boolean equalsTo(Expr expr) {
    CreateTable another = (CreateTable) expr;
    return tableName.equals(another.tableName) &&
        TUtil.checkEquals(tableElements, another.tableElements) &&
        TUtil.checkEquals(storageType, another.storageType) &&
        TUtil.checkEquals(location, another.location) &&
        TUtil.checkEquals(subquery, another.subquery) &&
        TUtil.checkEquals(params, another.params);
  }

  public static class ColumnDefinition extends DataType {
    String col_name;

    public ColumnDefinition(String columnName, String dataType) {
      super(dataType);
      this.col_name = columnName;
    }

    public ColumnDefinition(String columnName, DataType dataType) {
      super(dataType.getTypeName());
      if (dataType.hasLengthOrPrecision()) {
        setLengthOrPrecision(dataType.lengthOrPrecision);
        if (dataType.hasScale()) {
          setScale(dataType.scale);
        }
      }
      this.col_name = columnName;
    }

    public String getColumnName() {
      return this.col_name;
    }

    public boolean equals(Object obj) {
      if (obj instanceof ColumnDefinition) {
        ColumnDefinition another = (ColumnDefinition) obj;
        return col_name.equals(another.col_name) && super.equals(another);
      }

      return false;
    }
  }
}
