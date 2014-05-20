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

import java.util.Map;

public class CreateDatabase extends Expr {
  @Expose @SerializedName("DatabaseName")
  private String databaseName;
  @Expose @SerializedName("TablespaceName")
  private String tablespaceName;
  @Expose @SerializedName("IfNotExists")
  private boolean ifNotExists;
  @Expose @SerializedName("DatabaseProperties")
  private Map<String, String> params;

  public CreateDatabase(final String databaseName, final String tablespaceName, final boolean ifNotExists) {
    super(OpType.CreateDatabase);
    this.databaseName = databaseName;
    this.tablespaceName = tablespaceName;
    this.ifNotExists = ifNotExists;
  }

  public String getDatabaseName() {
    return this.databaseName;
  }

  public boolean hasTablespaceName() {
    return tablespaceName != null;
  }

  public String getTablespaceName() {
    return tablespaceName;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public boolean hasParams() {
    return params != null;
  }

  public void setParams(Map<String, String> params) {
    this.params = params;
  }

  public Map<String, String> getParams() {
    return params;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(databaseName, tablespaceName, ifNotExists);
  }

  @Override
  boolean equalsTo(Expr expr) {
    CreateDatabase another = (CreateDatabase) expr;
    return databaseName.equals(another.databaseName) &&
        TUtil.checkEquals(tablespaceName, another.tablespaceName) &&
        ifNotExists == another.ifNotExists;

  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    CreateDatabase createDb = (CreateDatabase) super.clone();
    createDb.databaseName = databaseName;
    createDb.tablespaceName = tablespaceName;
    createDb.ifNotExists = ifNotExists;
    return createDb;
  }
}
