/*
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

public class QualifiedAsteriskExpr extends Expr {
  private final static String ASTERISK = "*";
  @Expose @SerializedName("Qualifier")
  private String qualifier;

  public QualifiedAsteriskExpr() {
    super(OpType.Asterisk);
  }

  public QualifiedAsteriskExpr(String qualifier) {
    this();
    setQualifier(qualifier);
  }

  public boolean hasQualifier() {
    return this.qualifier != null;
  }

  public void setQualifier(String qualifier) {
    this.qualifier = qualifier;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(qualifier, ASTERISK);
  }

  @Override
  boolean equalsTo(Expr expr) {
    QualifiedAsteriskExpr another = (QualifiedAsteriskExpr) expr;
    return TUtil.checkEquals(this.qualifier, another.qualifier);
  }

  public String getQualifier() {
    return qualifier;
  }

  @Override
  public String toString() {
    return hasQualifier() ? qualifier + "." + ASTERISK : ASTERISK;
  }

  @Override
  public QualifiedAsteriskExpr clone() throws CloneNotSupportedException {
    QualifiedAsteriskExpr qualifiedAsteriskExpr = (QualifiedAsteriskExpr) super.clone();
    qualifiedAsteriskExpr.qualifier = qualifier;
    return qualifiedAsteriskExpr;
  }
}
