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

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.TUtil;

public abstract class FuncEval extends EvalNode implements Cloneable {
	@Expose protected FunctionDesc funcDesc;
	@Expose protected EvalNode [] argEvals;

	public FuncEval(Type type, FunctionDesc funcDesc, EvalNode[] argEvals) {
		super(type);
		this.funcDesc = funcDesc;
		this.argEvals = argEvals;
	}

  @Override
  public EvalContext newContext() {
    FuncCallCtx newCtx = new FuncCallCtx(argEvals);
    return newCtx;
  }
	
	public EvalNode [] getArgs() {
	  return this.argEvals;
	}

  public void setArgs(EvalNode [] args) {
    this.argEvals = args;
  }
	
	public DataType [] getValueType() {
		return this.funcDesc.getReturnType();
	}

	@Override
	public abstract void eval(EvalContext ctx, Schema schema, Tuple tuple);

  public abstract Datum terminate(EvalContext ctx);

	@Override
	public String getName() {
		return funcDesc.getSignature();
	}

  @Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(int i=0; i < argEvals.length; i++) {
			sb.append(argEvals[i]);
			if(i+1 < argEvals.length)
				sb.append(",");
		}
		return funcDesc.getSignature()+"("+sb+")";
	}
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof FuncEval) {
      FuncEval other = (FuncEval) obj;

      boolean b1 = this.type == other.type;
      boolean b2 = TUtil.checkEquals(funcDesc, other.funcDesc);
      boolean b3 = TUtil.checkEquals(argEvals, other.argEvals);
      return b1 && b2 && b3;
	  }
	  
	  return false;
	}
	
	@Override
	public int hashCode() {
	  return Objects.hashCode(funcDesc, argEvals);
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
    FuncEval eval = (FuncEval) super.clone();
    eval.funcDesc = (FunctionDesc) funcDesc.clone();
    eval.argEvals = new EvalNode[argEvals.length];
    for (int i = 0; i < argEvals.length; i++) {
      eval.argEvals[i] = (EvalNode) argEvals[i].clone();
    }    
    return eval;
  }
	
	@Override
  public void preOrder(EvalNodeVisitor visitor) {
    for (EvalNode eval : argEvals) {
      eval.postOrder(visitor);
    }
    visitor.visit(this);
  }
	
	@Override
	public void postOrder(EvalNodeVisitor visitor) {
	  for (EvalNode eval : argEvals) {
	    eval.postOrder(visitor);
	  }
	  visitor.visit(this);
	}

  protected class FuncCallCtx implements EvalContext {
    EvalContext [] argCtxs;
    FuncCallCtx(EvalNode [] argEvals) {
      argCtxs = new EvalContext[argEvals.length];
      for (int i = 0; i < argEvals.length; i++) {
        argCtxs[i] = argEvals[i].newContext();
      }
    }
  }
}