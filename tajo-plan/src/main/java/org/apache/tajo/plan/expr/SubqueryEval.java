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

package org.apache.tajo.plan.expr;

import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.logical.TableSubQueryNode;
import org.apache.tajo.storage.Tuple;

/**
 * SubqueryEval is a temporal eval to keep subquery information when the subquery occurs in expressions,
 * such as in subquery or scalar subquery, before {@link org.apache.tajo.plan.rewrite.rules.InSubqueryRewriteRule} is
 * applied.
 * During in subquery rewrite phase, A SubqueryEval is expected to be replaced with a Join.
 *
 */
public class SubqueryEval extends ValueSetEval {

  private TableSubQueryNode subQueryNode;

  public SubqueryEval(TableSubQueryNode subQueryNode) {
    super(EvalType.SUBQUERY);
    this.subQueryNode = subQueryNode;
  }

  @Override
  public DataType getValueType() {
    return subQueryNode.getOutSchema().getColumn(0).getDataType();
  }

  @Override
  public String getName() {
    return "SUBQUERY";
  }

  @Override
  public EvalNode bind(@Nullable EvalContext evalContext, Schema schema) {
    throw new UnsupportedException("Cannot call bind()");
  }

  @Override
  public Datum eval(Tuple tuple) {
    throw new UnsupportedException("Cannot call eval()");
  }

  public TableSubQueryNode getSubQueryNode() {
    return subQueryNode;
  }

  @Override
  public int hashCode() {
    return subQueryNode.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof SubqueryEval) {
      SubqueryEval other = (SubqueryEval) o;
      return this.subQueryNode.equals(other.subQueryNode);
    }
    return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    SubqueryEval clone = (SubqueryEval) super.clone();
    clone.subQueryNode = (TableSubQueryNode) this.subQueryNode.clone();
    return clone;
  }

  @Override
  public String toString() {
    return subQueryNode.toString();
  }

  @Override
  public Datum[] getValues() {
    throw new UnsupportedException("Cannot call getValues()");
  }
}
