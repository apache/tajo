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
package tajo.engine.planner.logical;

import com.google.gson.annotations.Expose;
import tajo.engine.eval.EvalNode;
import tajo.engine.json.GsonCreator;
import tajo.engine.parser.QueryBlock;
import tajo.engine.planner.JoinType;

public class JoinNode extends BinaryNode implements Cloneable {
  @Expose private JoinType joinType;
  @Expose private EvalNode joinQual;
  @Expose private QueryBlock.Target[] targets;

  public JoinNode(JoinType joinType, LogicalNode left) {
    super(ExprType.JOIN);
    this.joinType = joinType;
    setOuter(left);
  }

  public JoinNode(JoinType joinType, LogicalNode left, LogicalNode right) {
    super(ExprType.JOIN);
    this.joinType = joinType;
    setOuter(left);
    setInner(right);
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

  public boolean hasTargetList() {
    return this.targets != null;
  }

  public QueryBlock.Target[] getTargets() {
    return this.targets;
  }

  public void setTargetList(QueryBlock.Target[] targets) {
    this.targets = targets;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof JoinNode) {
      JoinNode other = (JoinNode) obj;
      return super.equals(other) && outer.equals(other.outer)
          && inner.equals(other.inner);
    } else {
      return false;
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    JoinNode join = (JoinNode) super.clone();
    join.joinType = this.joinType;
    join.joinQual = this.joinQual == null ? null : (EvalNode) this.joinQual.clone();
    return join;
  }

  public String toString() {
    return "\"Join\": \"joinType\": \"" + joinType +"\""
        + (joinQual != null ? ", \"qual\": " + joinQual : "")
        + "\n\"out schema: " + getOutSchema()
        + "\n\"in schema: " + getInSchema()
    		+ "\n" + getOuterNode().toString() + " and " + getInnerNode();
  }

  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
}
