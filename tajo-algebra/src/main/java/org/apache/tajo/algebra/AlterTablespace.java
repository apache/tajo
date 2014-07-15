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

public class AlterTablespace extends Expr {

  @Expose @SerializedName("TablespaceName")
  private String tablespaceName;
  @Expose @SerializedName("AlterTablespaceType")
  private AlterTablespaceSetType setType;

  @Expose @SerializedName("URI")
  private String uri;

  public AlterTablespace(final String tablespaceName) {
    super(OpType.AlterTablespace);
    this.tablespaceName = tablespaceName;
  }

  public String getTablespaceName() {
    return tablespaceName;
  }

  public AlterTablespaceSetType getSetType() {
    return setType;
  }

  public String getLocation() {
    return uri;
  }

  public void setLocation(String uri) {
    this.setType = AlterTablespaceSetType.LOCATION;
    this.uri = uri;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tablespaceName, setType);
  }

  @Override
  boolean equalsTo(Expr expr) {
    AlterTablespace another = (AlterTablespace) expr;
    return tablespaceName.equals(another.tablespaceName) &&
        TUtil.checkEquals(setType, another.setType) &&
        TUtil.checkEquals(uri, another.uri);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    AlterTablespace alter = (AlterTablespace) super.clone();
    alter.setType = setType;
    alter.tablespaceName = tablespaceName;
    alter.uri = uri;
    return alter;
  }
}
