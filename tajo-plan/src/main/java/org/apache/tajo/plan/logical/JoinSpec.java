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

package org.apache.tajo.plan.logical;

import com.google.common.base.Objects;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.util.TUtil;

import java.util.Set;

public class JoinSpec implements Cloneable {
  private JoinType type = null;
  private Set<EvalNode> predicates = TUtil.newHashSet();

  public JoinSpec() {

  }

  public JoinSpec(JoinType type) {
    this.type = type;
  }

  public void addPredicate(EvalNode predicate) {
    this.predicates.add(predicate);
  }

  public void addPredicates(Set<EvalNode> predicates) {
    this.predicates.addAll(predicates);
  }

  public boolean hasPredicates() {
    return predicates.size() > 0;
  }

  public void setPredicates(Set<EvalNode> predicates) {
    this.predicates.clear();
    this.predicates.addAll(predicates);
  }

  public void setSingletonPredicate(EvalNode predicates) {
    this.setPredicates(TUtil.newHashSet(AlgebraicUtil.toConjunctiveNormalFormArray(predicates)));
  }

  public EvalNode getSingletonPredicate() {
    if (predicates.size() > 1) {
      return AlgebraicUtil.createSingletonExprFromCNF(predicates.toArray(new EvalNode[predicates.size()]));
    } else if (predicates.size() == 1) {
      return predicates.iterator().next();
    } else {
      return null;
    }
  }

  public Set<EvalNode> getPredicates() {
    return predicates;
  }

  public JoinType getType() {
    return type;
  }

  public void setType(JoinType type) {
    this.type = type;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof JoinSpec) {
      JoinSpec other = (JoinSpec) o;
      return this.type == other.type && this.predicates.equals(other.predicates);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type, predicates.hashCode());
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    JoinSpec clone = new JoinSpec(this.type);
    clone.setPredicates(this.predicates);
    return clone;
  }
}
