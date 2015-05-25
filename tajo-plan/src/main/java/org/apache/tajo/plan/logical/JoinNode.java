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
import org.apache.tajo.plan.expr.BinaryEval;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.util.TUtil;

import java.util.Arrays;

public class JoinNode extends BinaryNode implements Projectable, Cloneable {
  @Expose private JoinType joinType;
  @Expose private EvalNode joinQual;
  @Expose private Target[] targets;

  public JoinNode(int pid) {
    super(pid, NodeType.JOIN);
  }

  public void init(JoinType joinType, LogicalNode left, LogicalNode right) {
    this.joinType = joinType;
    setLeftChild(left);
    setRightChild(right);
  }

  public JoinType getJoinType() {
    return this.joinType;
  }

  public void setJoinType(JoinType joinType) {
    this.joinType = joinType;
  }

  public void setJoinQual(EvalNode joinQual) {
    this.joinQual = joinQual;
  }

  public boolean hasJoinQual() {
    return this.joinQual != null;
  }

  public EvalNode getJoinQual() {
    return this.joinQual;
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
    PlanString planStr = new PlanString(this).appendTitle("(").appendTitle(joinType.name()).appendTitle(")");
    if (hasJoinQual()) {
      planStr.addExplan("Join Cond: " + joinQual.toString());
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
    result = prime * result + ((joinQual == null) ? 0 : joinQual.hashCode());
    result = prime * result + ((joinType == null) ? 0 : joinType.hashCode());
    result = prime * result + Arrays.hashCode(targets);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof JoinNode) {
      JoinNode other = (JoinNode) obj;
      boolean eq = this.joinType.equals(other.joinType);
      eq &= TUtil.checkEquals(this.targets, other.targets);
      eq &= TUtil.checkEquals(joinQual, other.joinQual);
      return eq && leftChild.equals(other.leftChild) && rightChild.equals(other.rightChild);
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    JoinNode join = (JoinNode) super.clone();
    join.joinType = this.joinType;
    join.joinQual = this.joinQual == null ? null : (BinaryEval) this.joinQual.clone();
    if (hasTargets()) {
      join.targets = new Target[targets.length];
      for (int i = 0; i < targets.length; i++) {
        join.targets[i] = (Target) targets[i].clone();
      }
    }
    return join;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("Join (type").append(joinType);
    if (hasJoinQual()) {
      sb.append(",filter=").append(joinQual);
    }
    sb.append(")");
    return sb.toString();
  }
}
