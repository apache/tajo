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
import org.apache.tajo.plan.PlanString;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.util.TUtil;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class EvalExprNode extends LogicalNode implements Projectable {
  @Expose private List<Target> exprs = null;

  public EvalExprNode(int pid) {
    super(pid, NodeType.EXPRS);
  }

  @Override
  public int childNum() {
    return 0;
  }

  @Override
  public LogicalNode getChild(int idx) {
    return null;
  }

  @Override
  public boolean hasTargets() {
    return true;
  }

  @Override
  public void setTargets(List<Target> targets) {
    this.exprs = targets;
    this.setOutSchema(PlannerUtil.targetToSchema(targets));
  }

  @Override
  public List<Target> getTargets() {
    return exprs;
  }

  @Override
  public Stream<Target> targets() {
    return getTargets().stream();
  }

  public List<Target> getExprs() {
    return this.exprs;
  }
  
  @Override
  public String toString() {
    return "EvalExprNode (" + StringUtils.join(exprs) + ")";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Objects.hashCode(exprs);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof EvalExprNode) {
      EvalExprNode other = (EvalExprNode) obj;
      return TUtil.checkEquals(this.exprs, other.exprs);
    } else {
      return false;
    }
  }
  
  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    // nothing
  }

  @Override
  public void postOrder(LogicalNodeVisitor visitor) {
    // nothing
  }

  @Override
  public PlanString getPlanString() {
    return null;
  }
}
