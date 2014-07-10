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

public class ColumnReferenceExpr extends Expr {
  @Expose @SerializedName("Qualifier")
  private String qualifier;
  @Expose @SerializedName("ColumnName")
  private String name;

  public ColumnReferenceExpr(String referenceName) {
    super(OpType.Column);
    setName(referenceName);
  }

  public ColumnReferenceExpr(String qualifier, String columnName) {
    super(OpType.Column);
    this.qualifier = qualifier;
    this.name = columnName;
  }

  public boolean hasQualifier() {
    return this.qualifier != null;
  }

  public void setQualifier(String qualifier) {
    this.qualifier = qualifier;
  }

  public String getQualifier() {
    return this.qualifier;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String qualifiedName) {
    String [] parts = qualifiedName.split("\\.");

    if (parts.length > 1) {
      qualifier = qualifiedName.substring(0, qualifiedName.lastIndexOf("."));
      name = qualifiedName.substring(qualifiedName.lastIndexOf(".") + 1, qualifiedName.length());
    } else {
      name = parts[0];
    }
  }

  public String getCanonicalName() {
    if (qualifier != null) {
      return qualifier + "." + name;
    } else {
      return name;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, qualifier);
  }

  @Override
  public boolean equalsTo(Expr expr) {
    ColumnReferenceExpr another = (ColumnReferenceExpr) expr;
    return name.equals(another.name) &&
        TUtil.checkEquals(qualifier, another.qualifier);
  }

  @Override
  public String toString() {
    return qualifier != null ? qualifier + "." + name : name;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    ColumnReferenceExpr column = (ColumnReferenceExpr) super.clone();
    column.qualifier = qualifier;
    column.name = name;
    return column;
  }
}
