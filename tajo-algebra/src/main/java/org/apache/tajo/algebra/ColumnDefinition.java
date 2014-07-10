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

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public  class ColumnDefinition extends DataTypeExpr {
  @Expose @SerializedName("ColumnDefName")
  String columnName;

  public ColumnDefinition(String columnName, String dataType) {
    super(dataType);
    this.columnName = columnName;
  }

  public ColumnDefinition(String columnName, DataTypeExpr dataType) {
    super(dataType.getTypeName());
    if (dataType.hasLengthOrPrecision()) {
      setLengthOrPrecision(dataType.lengthOrPrecision);
      if (dataType.hasScale()) {
        setScale(dataType.scale);
      }
    }
    this.columnName = columnName;
  }

  public String getColumnName() {
    return this.columnName;
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    return hash * 89 * columnName.hashCode();

  }

  @Override
  public boolean equalsTo(Expr expr) {
    if (expr instanceof ColumnDefinition) {
      ColumnDefinition another = (ColumnDefinition) expr;
      return columnName.equals(another.columnName) && super.equalsTo(another);
    }

    return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    ColumnDefinition column = (ColumnDefinition) super.clone();
    column.columnName = columnName;
    return column;
  }
}
