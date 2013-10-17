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

package org.apache.tajo.engine.eval;

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.storage.Tuple;

public abstract class EvalNode implements Cloneable, GsonObject {
	@Expose protected EvalType type;
	@Expose protected EvalNode leftExpr;
	@Expose protected EvalNode rightExpr;
	
	public EvalNode(EvalType type) {
		this.type = type;
	}
	
	public EvalNode(EvalType type, EvalNode left, EvalNode right) {
		this(type);
		this.leftExpr = left;
		this.rightExpr = right;
	}

  public abstract EvalContext newContext();
	
	public EvalType getType() {
		return this.type;
	}
	
	public void setLeftExpr(EvalNode expr) {
		this.leftExpr = expr;
	}
	
	public <T extends EvalNode> T getLeftExpr() {
		return (T) this.leftExpr;
	}
	
	public void setRightExpr(EvalNode expr) {
		this.rightExpr = expr;
	}
	
	public <T extends EvalNode> T getRightExpr() {
		return (T) this.rightExpr;
	}

  public EvalNode getExpr(int id) {
    if (id == 0) {
      return this.leftExpr;
    } else if (id == 1) {
      return this.rightExpr;
    } else {
      throw new ArrayIndexOutOfBoundsException("only 0 or 1 is available (" + id + " is not available)");
    }
  }
	
	public abstract DataType getValueType();
	
	public abstract String getName();
	
	public String toString() {
		return "(" + this.type + "(" + leftExpr.toString() + " " + rightExpr.toString() + "))";
	}

  @Override
	public String toJson() {
    return CoreGsonHelper.toJson(this, EvalNode.class);
	}
	
	public void eval(EvalContext ctx, Schema schema, Tuple tuple) {}

  public abstract Datum terminate(EvalContext ctx);

  @Deprecated
	public void preOrder(EvalNodeVisitor visitor) {
	  visitor.visit(this);
	  leftExpr.preOrder(visitor);
	  rightExpr.preOrder(visitor);
	}

  @Deprecated
	public void postOrder(EvalNodeVisitor visitor) {
	  leftExpr.postOrder(visitor);
	  rightExpr.postOrder(visitor);	  	  
	  visitor.visit(this);
	}

  public abstract boolean equals(Object obj);
	
	@Override
	public Object clone() throws CloneNotSupportedException {
	  EvalNode node = (EvalNode) super.clone();
	  node.type = type;
	  node.leftExpr = leftExpr != null ? (EvalNode) leftExpr.clone() : null;
	  node.rightExpr = rightExpr != null ? (EvalNode) rightExpr.clone() : null;
	  
	  return node;
	}
}
