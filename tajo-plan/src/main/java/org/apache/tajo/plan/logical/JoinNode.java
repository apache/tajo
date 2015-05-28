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

/**
 * 
 */
package org.apache.tajo.plan.logical;

import com.google.gson.annotations.Expose;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.plan.PlanString;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.util.TUtil;

import java.util.Arrays;

public class JoinNode extends BinaryNode implements Projectable, Cloneable {
  @Expose private JoinSpec joinSpec = new JoinSpec();
  @Expose private Target[] targets;

  public JoinNode(int pid) {
    super(pid, NodeType.JOIN);
  }

  public void init(JoinType joinType, LogicalNode left, LogicalNode right) {
    this.joinSpec.setType(joinType);
    setLeftChild(left);
    setRightChild(right);
  }

  public JoinType getJoinType() {
    return this.joinSpec.getType();
  }

  public JoinSpec getJoinSpec() {
    return joinSpec;
  }

  public void setJoinType(JoinType joinType) {
    this.joinSpec.setType(joinType);
  }

  public void setJoinQual(EvalNode joinQual) {
    this.joinSpec.setSingletonPredicate(joinQual);
  }

  public boolean hasJoinQual() {
    return this.joinSpec.hasPredicates();
  }

  public EvalNode getJoinQual() {
    return this.joinSpec.getSingletonPredicate();
  }

  @Override
  public boolean hasTargets() {
    return this.targets != null;
  }

  @Override
  public Target[] getTargets() {
    return this.targets;
  }

  @Override
  public void setTargets(Target[] targets) {
    this.targets = targets;
    this.setOutSchema(PlannerUtil.targetToSchema(targets));
  }

  @Override
  public PlanString getPlanString() {
    PlanString planStr = new PlanString(this).appendTitle("(").appendTitle(joinSpec.getType().name()).appendTitle(")");
    if (hasJoinQual()) {
      planStr.addExplan("Join Cond: " + joinSpec.getSingletonPredicate().toString());
    }

    if (hasTargets()) {
      planStr.addExplan("target list: ");
      boolean first = true;
      for (Target target : targets) {
        if (!first) {
          planStr.appendExplain(", ");
        }
        planStr.appendExplain(target.toString());
        first = false;
      }
    }

    planStr.addDetail("out schema: " + getOutSchema());
    planStr.addDetail("in schema: " + getInSchema());

    return planStr;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + joinSpec.hashCode();
    result = prime * result + Arrays.hashCode(targets);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof JoinNode) {
      JoinNode other = (JoinNode) obj;
      boolean eq = this.joinSpec.equals(other.joinSpec);
      eq &= TUtil.checkEquals(this.targets, other.targets);
      return eq && leftChild.equals(other.leftChild) && rightChild.equals(other.rightChild);
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    JoinNode join = (JoinNode) super.clone();
    join.joinSpec = (JoinSpec) this.joinSpec.clone();
    if (hasTargets()) {
      join.targets = new Target[targets.length];
      for (int i = 0; i < targets.length; i++) {
        join.targets[i] = (Target) targets[i].clone();
      }
    }
    return join;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("Join (type").append(joinSpec.getType());
    if (hasJoinQual()) {
      sb.append(",filter=").append(joinSpec.getSingletonPredicate());
    }
    sb.append(")");
    return sb.toString();
  }
}
