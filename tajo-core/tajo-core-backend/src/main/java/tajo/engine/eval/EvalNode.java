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

package tajo.engine.eval;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import tajo.catalog.Schema;
import tajo.common.TajoDataTypes.DataType;
import tajo.datum.Datum;
import tajo.engine.json.GsonCreator;
import tajo.storage.Tuple;

public abstract class EvalNode implements Cloneable {
	@Expose
	protected Type type;
	@Expose
	protected EvalNode leftExpr;
	@Expose
	protected EvalNode rightExpr;
	
	public EvalNode(Type type) {
		this.type = type;
	}
	
	public EvalNode(Type type, EvalNode left, EvalNode right) {
		this(type);
		this.leftExpr = left;
		this.rightExpr = right;
	}

  public abstract EvalContext newContext();
	
	public Type getType() {
		return this.type;
	}
	
	public void setLeftExpr(EvalNode expr) {
		this.leftExpr = expr;
	}
	
	public EvalNode getLeftExpr() {
		return this.leftExpr;
	}
	
	public void setRightExpr(EvalNode expr) {
		this.rightExpr = expr;
	}
	
	public EvalNode getRightExpr() {
		return this.rightExpr;
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
	
	public abstract DataType [] getValueType();
	
	public abstract String getName();
	
	public String toString() {
		return "("+this.type+"("+leftExpr.toString()+" "+rightExpr.toString()+"))";
	}
	
	public String toJSON() {
	  Gson gson = GsonCreator.getInstance();
    return gson.toJson(this, EvalNode.class);
	}
	
	public void eval(EvalContext ctx, Schema schema, Tuple tuple) {}

  public abstract Datum terminate(EvalContext ctx);

	public void preOrder(EvalNodeVisitor visitor) {
	  visitor.visit(this);
	  leftExpr.preOrder(visitor);
	  rightExpr.preOrder(visitor);
	}
	
	public void postOrder(EvalNodeVisitor visitor) {
	  leftExpr.postOrder(visitor);
	  rightExpr.postOrder(visitor);	  	  
	  visitor.visit(this);
	}
	
	public static enum Type {
    AGG_FUNCTION,
    AND,
	  OR,
	  EQUAL("="),
    IS,
	  NOT_EQUAL("<>"),
	  LTH("<"),
	  LEQ("<="),
	  GTH(">"),
	  GEQ(">="),
	  NOT("!"),
	  PLUS("+"),
    MINUS("-"),
    MODULAR("%"),
    MULTIPLY("*"),
    DIVIDE("/"),
	  FIELD,
    FUNCTION,
    LIKE,
    CONST,
    CASE,
    WHEN;

    private String represent;
    Type() {
    }
    Type(String represent) {
      this.represent = represent;
    }

    public String toString() {
      return represent == null ? this.name() : represent;
    }
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
