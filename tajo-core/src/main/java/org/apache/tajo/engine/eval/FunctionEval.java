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
import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.TUtil;

import static org.apache.tajo.catalog.proto.CatalogProtos.FunctionType.DISTINCT_AGGREGATION;
import static org.apache.tajo.catalog.proto.CatalogProtos.FunctionType.DISTINCT_UDA;

public abstract class FunctionEval extends EvalNode implements Cloneable {
  public static enum ParamType {
    CONSTANT, VARIABLE, NULL
  }

  @Expose protected FunctionDesc funcDesc;
	@Expose protected EvalNode [] argEvals;

	public FunctionEval(EvalType type, FunctionDesc funcDesc, EvalNode[] argEvals) {
		super(type);
		this.funcDesc = funcDesc;
    Preconditions.checkArgument(argEvals != null, "argEvals cannot be null");
    this.argEvals = argEvals;
	}

  public FunctionDesc getFuncDesc() {
    return funcDesc;
  }

  public ParamType [] getParamType() {
    ParamType [] paramTypes = new ParamType[argEvals.length];
    for (int i = 0; i < argEvals.length; i++) {
      if (argEvals[i].getType() == EvalType.CONST) {
        if (argEvals[i].getValueType().getType() == TajoDataTypes.Type.NULL_TYPE) {
          paramTypes[i] = ParamType.NULL;
        } else {
          paramTypes[i] = ParamType.CONSTANT;
        }
      } else {
        paramTypes[i] = ParamType.VARIABLE;
      }
    }
    return paramTypes;
  }

  public boolean isDistinct() {
    return funcDesc.getFuncType() == DISTINCT_AGGREGATION || funcDesc.getFuncType() == DISTINCT_UDA;
  }

	public EvalNode [] getArgs() {
	  return this.argEvals;
	}

  public void setArg(int idx, EvalNode arg) {
    this.argEvals[idx] = arg;
  }

  public void setArgs(EvalNode [] args) {
    this.argEvals = args;
  }

  @Override
  public int childNum() {
    if (argEvals != null) {
      return argEvals.length;
    } else {
      return 0;
    }
  }

  @Override
  public EvalNode getChild(int idx) {
    return argEvals[idx];
  }


	public DataType getValueType() {
		return this.funcDesc.getReturnType();
	}

	@Override
	public abstract Datum eval(Schema schema, Tuple tuple);

	@Override
	public String getName() {
		return funcDesc.getFunctionName();
	}

  @Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(int i=0; i < argEvals.length; i++) {
			sb.append(argEvals[i]);
			if(i+1 < argEvals.length)
				sb.append(",");
		}
		return funcDesc.getFunctionName() + "(" + (isDistinct() ? " distinct " : "") + sb+")";
	}
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof FunctionEval) {
      FunctionEval other = (FunctionEval) obj;

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
    FunctionEval eval = (FunctionEval) super.clone();
    eval.funcDesc = (FunctionDesc) funcDesc.clone();
    if (argEvals != null) {
      eval.argEvals = new EvalNode[argEvals.length];
      for (int i = 0; i < argEvals.length; i++) {
        eval.argEvals[i] = (EvalNode) argEvals[i].clone();
      }
    }
    return eval;
  }
	
	@Override
  public void preOrder(EvalNodeVisitor visitor) {
    if (argEvals != null) {
      for (EvalNode eval : argEvals) {
        eval.postOrder(visitor);
      }
    }
    visitor.visit(this);
  }
	
	@Override
	public void postOrder(EvalNodeVisitor visitor) {
    if (argEvals != null) {
      for (EvalNode eval : argEvals) {
        eval.postOrder(visitor);
      }
    }
	  visitor.visit(this);
	}
}