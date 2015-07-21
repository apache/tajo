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

package org.apache.tajo.plan.joinorder;

import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.JoinSpec;
import org.apache.tajo.util.StringUtils;

import java.util.Set;

public class JoinEdge {
  private final JoinSpec joinSpec;
  private final JoinVertex leftVertex;
  private final JoinVertex rightVertex;
  private final Schema schema;

  public JoinEdge(JoinSpec joinSpec, JoinVertex leftVertex, JoinVertex rightVertex) {
    this.joinSpec = joinSpec;
    this.leftVertex = leftVertex;
    this.rightVertex = rightVertex;
    this.schema = SchemaUtil.merge(leftVertex.getSchema(), rightVertex.getSchema());
  }

  public JoinType getJoinType() {
    return joinSpec.getType();
  }

  public boolean hasJoinQual() {
    return joinSpec.hasPredicates();
  }

  public void addJoinQual(EvalNode joinQual) {
    this.joinSpec.addPredicate(joinQual);
  }

  public void addJoinPredicates(Set<EvalNode> predicates) {
    this.joinSpec.addPredicates(predicates);
  }

  public Set<EvalNode> getJoinQual() {
    return joinSpec.getPredicates();
  }

  public JoinSpec getJoinSpec() {
    return this.joinSpec;
  }

  public EvalNode getSingletonJoinQual() {
    return joinSpec.getSingletonPredicate();
  }

  public String toString() {
    return leftVertex + " " + joinSpec.getType() + " " + rightVertex + " ON " +
        StringUtils.join(joinSpec.getPredicates(), ", ");
  }

  public JoinVertex getLeftVertex() {
    return leftVertex;
  }

  public JoinVertex getRightVertex() {
    return rightVertex;
  }

  public Schema getSchema() {
    return schema;
  }
}
