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
package org.apache.tajo.engine.planner.logical;

import com.google.gson.annotations.Expose;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.util.TUtil;

public class EvalExprNode extends LogicalNode implements Projectable {
  @Expose private Target[] exprs;

  public EvalExprNode(Target[] exprs) {
    super(ExprType.EXPRS);
    this.exprs = exprs;
  }

  @Override
  public boolean hasTargets() {
    return true;
  }

  @Override
  public void setTargets(Target[] targets) {
    this.exprs = targets;
  }

  @Override
  public Target[] getTargets() {
    return exprs;
  }

  public Target[] getExprs() {
    return this.exprs;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\"EvalExpr\": {");
    sb.append("\"targets\": [");

    for (int i = 0; i < exprs.length; i++) {
      sb.append("\"").append(exprs[i]).append("\"");
      if( i < exprs.length - 1) {
        sb.append(",");
      }
    }
    sb.append("],");
    sb.append("\n  \"out schema\": ").append(getOutSchema()).append(",");
    sb.append("\n  \"in schema\": ").append(getInSchema());
    sb.append("}");
    return sb.toString();
  }

  public boolean equals(Object object) {
    if (object instanceof EvalExprNode) {
      EvalExprNode other = (EvalExprNode) object;
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
}
