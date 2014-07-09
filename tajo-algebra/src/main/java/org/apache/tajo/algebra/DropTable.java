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

public class DropTable extends Expr {
  @Expose @SerializedName("TableName")
  private String tableName;
  @Expose @SerializedName("IfExists")
  private boolean ifExists;
  @Expose @SerializedName("IsPurge")
  private boolean purge;

  public DropTable(String tableName, boolean ifExists, boolean purge) {
    super(OpType.DropTable);
    this.tableName = tableName;
    this.ifExists = ifExists;
    this.purge = purge;
  }

  public String getTableName() {
    return this.tableName;
  }

  public boolean isIfExists() {
    return ifExists;
  }

  public boolean isPurge() {
    return purge;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName, ifExists, purge);
  }

  @Override
  boolean equalsTo(Expr expr) {
    if (expr instanceof DropTable) {
      DropTable another = (DropTable) expr;
      return tableName.equals(another.tableName) &&
          ifExists == another.ifExists &&
          purge == another.purge;
    }
    return false;
  }

  public Object clone() throws CloneNotSupportedException {
    DropTable drop = (DropTable) super.clone();
    drop.tableName = tableName;
    drop.ifExists = ifExists;
    drop.purge = purge;
    return drop;
  }
}
