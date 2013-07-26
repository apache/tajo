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

public class ColumnReferenceExpr extends Expr {
  private String tableName;
  private String name;

  public ColumnReferenceExpr(String columnName) {
    super(OpType.Column);
    this.name = columnName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getName() {
    return this.name;
  }

  public boolean hasTableName() {
    return this.tableName != null;
  }

  public String getTableName() {
    return this.tableName;
  }

  public String getCanonicalName() {
    if (tableName != null) {
      return tableName + "." + name;
    } else {
      return name;
    }
  }

  public boolean equalsTo(Expr expr) {
    ColumnReferenceExpr another = (ColumnReferenceExpr) expr;
    return name.equals(another.name) &&
        TUtil.checkEquals(tableName, another.tableName);
  }
}
