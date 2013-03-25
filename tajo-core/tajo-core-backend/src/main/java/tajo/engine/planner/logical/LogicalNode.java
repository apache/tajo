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
import tajo.catalog.Schema;

public abstract class LogicalNode implements Cloneable {
	@Expose
	private ExprType type;
	@Expose
	private Schema inputSchema;
	@Expose
	private Schema outputSchema;

	@Expose
	private double cost = 0;
	
	public LogicalNode() {
		
	}

	public LogicalNode(ExprType type) {
		this.type = type;
	}
	
	public ExprType getType() {
		return this.type;
	}

	public void setType(ExprType type) {
		this.type = type;
	}

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

      boolean b1 = this.type == other.type;
      boolean b2 = this.inputSchema.equals(other.inputSchema);
      boolean b3 = this.outputSchema.equals(other.outputSchema);
      boolean b4 = this.cost == other.cost;
      
      return b1 && b2 && b3 && b4;
	  } else {
	    return false;
	  }
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
	  LogicalNode node = (LogicalNode)super.clone();
	  node.type = type;
	  node.inputSchema = 
	      (Schema) (inputSchema != null ? inputSchema.clone() : null);
	  node.outputSchema = 
	      (Schema) (outputSchema != null ? outputSchema.clone() : null);
	  
	  return node;
	}
	
	public abstract String toJSON();

	public abstract void preOrder(LogicalNodeVisitor visitor);
  public abstract void postOrder(LogicalNodeVisitor visitor);
}
