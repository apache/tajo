/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.planner;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.engine.eval.EvalNode;

public class JoinClause implements Cloneable {
  @Expose private JoinType joinType;
  @Expose private FromTable left;
  @Expose private FromTable right;
  @Expose private JoinClause leftJoin;
  @Expose private EvalNode joinQual;
  @Expose private Column[] joinColumns;
  @Expose private boolean natural = false;

  @SuppressWarnings("unused")
  public JoinClause() {
    // for gson
  }

  public JoinClause(final JoinType joinType) {
    this.joinType = joinType;
  }

  public JoinClause(final JoinType joinType, final FromTable right) {
    this.joinType = joinType;
    this.right = right;
  }

  public JoinClause(final JoinType joinType, final FromTable left,
                    final FromTable right) {
    this(joinType, right);
    this.left = left;
  }

  public JoinType getJoinType() {
    return this.joinType;
  }

  public void setNatural() {
    this.natural = true;
  }

  public boolean isNatural() {
    return this.natural;
  }

  public void setRight(FromTable right) {
    this.right = right;
  }

  public void setLeft(FromTable left) {
    this.left = left;
  }

  public void setLeft(JoinClause left) {
    this.leftJoin = left;
  }

  public boolean hasLeftJoin() {
    return leftJoin != null;
  }

  public FromTable getLeft() {
    return this.left;
  }

  public FromTable getRight() {
    return this.right;
  }

  public JoinClause getLeftJoin() {
    return this.leftJoin;
  }

  public void setJoinQual(EvalNode qual) {
    this.joinQual = qual;
  }

  public boolean hasJoinQual() {
    return this.joinQual != null;
  }

  public EvalNode getJoinQual() {
    return this.joinQual;
  }

  public void setJoinColumns(Column [] columns) {
    this.joinColumns = columns;
  }

  public boolean hasJoinColumns() {
    return this.joinColumns != null;
  }

  public Column [] getJoinColumns() {
    return this.joinColumns;
  }


  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(this);
  }
}
