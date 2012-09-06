/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import tajo.catalog.FunctionDesc;
import tajo.catalog.Schema;
import tajo.datum.Datum;
import tajo.engine.function.GeneralFunction;
import tajo.engine.json.GsonCreator;
import tajo.engine.utils.TUtil;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

/**
 * @author Hyunsik Choi
 */
public class FuncCallEval extends FuncEval {
	@Expose protected GeneralFunction instance;
  private Tuple tuple;
  private Tuple params = null;
  private Schema schema;

	public FuncCallEval(FunctionDesc desc, GeneralFunction instance, EvalNode [] givenArgs) {
		super(Type.FUNCTION, desc, givenArgs);
		this.instance = instance;
  }

  /* (non-Javadoc)
    * @see nta.query.executor.eval.Expr#evalVal(Tuple)
    */
	@Override
	public void eval(EvalContext ctx, Schema schema, Tuple tuple) {
    this.schema = schema;
    this.tuple = tuple;
	}

  @Override
  public Datum terminate(EvalContext ctx) {
    FuncCallCtx localCtx = (FuncCallCtx) ctx;
    if (this.params == null) {
      params = new VTuple(argEvals.length);
    }

    if(argEvals != null) {
      params.clear();
      for(int i=0;i < argEvals.length; i++) {
        argEvals[i].eval(localCtx.argCtxs[i], schema, tuple);
        params.put(i, argEvals[i].terminate(localCtx.argCtxs[i]));
      }
    }
    return instance.eval(params);
  }

  @Override
	public String toJSON() {
	  Gson gson = GsonCreator.getInstance();
    return gson.toJson(this, EvalNode.class);
	}
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof FuncCallEval) {
      FuncCallEval other = (FuncCallEval) obj;
      return super.equals(other) &&
          TUtil.checkEquals(instance, other.instance);
	  }
	  
	  return false;
	}
	
	@Override
	public int hashCode() {
	  return Objects.hashCode(funcDesc, instance);
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
    FuncCallEval eval = (FuncCallEval) super.clone();
    eval.instance = (GeneralFunction) instance.clone();
    return eval;
  }
}