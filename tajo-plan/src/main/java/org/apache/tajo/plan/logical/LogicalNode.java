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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.plan.PlanString;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.serder.PlanGsonHelper;
import org.apache.tajo.util.TUtil;

public abstract class LogicalNode implements Cloneable, GsonObject {
  @Expose private int nodeId;
  @Expose private NodeType type;
	@Expose private Schema inputSchema;
	@Expose	private Schema outputSchema;

	@Expose	private double cost = 0;

	protected LogicalNode(int nodeId, NodeType type) {
    this.nodeId = nodeId;
    this.type = type;
	}

  public int getPID() {
    return nodeId;
  }

  public void setPID(int pid) {
    this.nodeId = pid;
  }
	
	public NodeType getType() {
		return this.type;
	}

	public void setType(NodeType type) {
		this.type = type;
	}

  public abstract int childNum();

  public abstract LogicalNode getChild(int idx);

	public double getCost() {
		return this.cost;
	}

	public void setCost(double cost) {
		this.cost = cost;
	}
	
	public void setInSchema(Schema inSchema) {
	  this.inputSchema = inSchema;
	}
	
	public Schema getInSchema() {
	  return this.inputSchema;
	}
	
	public void setOutSchema(Schema outSchema) {
	  this.outputSchema = outSchema;
	}
	
	public Schema getOutSchema() {
	  return this.outputSchema;
	}
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof LogicalNode) {
	    LogicalNode other = (LogicalNode) obj;

      boolean eq = this.type == other.type;
      eq = eq && TUtil.checkEquals(this.inputSchema, other.inputSchema);
      eq = eq && TUtil.checkEquals(this.outputSchema, other.outputSchema);
      eq = eq && this.cost == other.cost;

      return eq;
	  } else {
	    return false;
	  }
  }

  public boolean deepEquals(Object o) {
    return equals(o);
  }

	@Override
	public Object clone() throws CloneNotSupportedException {
	  LogicalNode node = (LogicalNode)super.clone();
    node.nodeId = nodeId;
	  node.type = type;
	  node.inputSchema =  (Schema) (inputSchema != null ? inputSchema.clone() : null);
	  node.outputSchema = (Schema) (outputSchema != null ? outputSchema.clone() : null);
	  return node;
	}

  @Override
  public String toJson() {
    return PlanGsonHelper.toJson(this, LogicalNode.class);
  }

	public abstract void preOrder(LogicalNodeVisitor visitor);
  public abstract void postOrder(LogicalNodeVisitor visitor);

  public abstract PlanString getPlanString();

  public String toString() {
    return PlannerUtil.buildExplainString(this);
  }
}
