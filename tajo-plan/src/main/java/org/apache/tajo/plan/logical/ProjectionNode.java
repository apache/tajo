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

package org.apache.tajo.plan.logical;

import java.util.Arrays;

import com.google.gson.annotations.Expose;

import org.apache.tajo.plan.PlanString;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.Target;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.util.TUtil;

public class ProjectionNode extends UnaryNode implements Projectable {

  @Expose private boolean distinct = false;
  /**
   * the targets are always filled even if the query is 'select *'
   */
  @Expose	private Target [] targets;

	public ProjectionNode(int pid) {
		super(pid, NodeType.PROJECTION);
	}

  public void init(boolean distinct, Target [] targets) {
    this.distinct = distinct;
    this.targets = targets;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public boolean hasTargets() {
    return this.targets != null;
  }

  @Override
  public void setTargets(Target[] targets) {
    this.targets = targets;
    this.setOutSchema(PlannerUtil.targetToSchema(targets));
  }

  @Override
  public Target [] getTargets() {
    return this.targets;
  }
	
	public void setChild(LogicalNode subNode) {
	  super.setChild(subNode);
	}
	
	public String toString() {
	  StringBuilder sb = new StringBuilder("Projection (distinct=").append(distinct);
    if (targets != null) {
      sb.append(", exprs=").append(StringUtils.join(targets)).append(")");
    }
	  return sb.toString();
	}
	
	@Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (distinct ? 1231 : 1237);
    result = prime * result + Arrays.hashCode(targets);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ProjectionNode) {
      ProjectionNode other = (ProjectionNode) obj;
      
      boolean b1 = super.equals(other);
      boolean b2 = TUtil.checkEquals(targets, other.targets);
      return b1 && b2;
    } else {
      return false;
    }
  }

	@Override
  public Object clone() throws CloneNotSupportedException {
	  ProjectionNode projNode = (ProjectionNode) super.clone();
	  projNode.targets = targets.clone();
	  
	  return projNode;
	}

  @Override
  public PlanString getPlanString() {
    PlanString planStr = new PlanString(this);

    if (distinct) {
      planStr.appendTitle(" (distinct)");
    }


    StringBuilder sb = new StringBuilder("Targets: ");
    if (targets != null) {
      for (int i = 0; i < targets.length; i++) {
        sb.append(targets[i]);
        if (i < targets.length - 1) {
          sb.append(", ");
        }
      }
    }
    planStr.addExplan(sb.toString());
    if (getOutSchema() != null) {
      planStr.addExplan("out schema: " + getOutSchema().toString());
    }
    if (getInSchema() != null) {
      planStr.addExplan("in  schema: " + getInSchema().toString());
    }

    return planStr;
  }
}
