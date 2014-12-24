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

package org.apache.tajo.plan.expr;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.TUtil;

import javax.annotation.Nullable;

public class GeneralFunctionEval extends FunctionEval {
  @Expose protected GeneralFunction instance;
  private Tuple params = null;

	public GeneralFunctionEval(@Nullable OverridableConf queryContext, FunctionDesc desc, GeneralFunction instance,
                             EvalNode[] givenArgs) {
		super(EvalType.FUNCTION, desc, givenArgs);
		this.instance = instance;
    this.instance.init(queryContext, getParamType());
  }

  /* (non-Javadoc)
    * @see nta.query.executor.eval.Expr#evalVal(Tuple)
    */
	@Override
	public Datum eval(Schema schema, Tuple tuple) {
    if (this.params == null) {
      params = new VTuple(argEvals.length);
    }
    if(argEvals != null) {
      params.clear();
      for(int i=0;i < argEvals.length; i++) {
        params.put(i, argEvals[i].eval(schema, tuple));
      }
    }

    return instance.eval(params);
	}
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof GeneralFunctionEval) {
      GeneralFunctionEval other = (GeneralFunctionEval) obj;
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
    GeneralFunctionEval eval = (GeneralFunctionEval) super.clone();
    eval.instance = (GeneralFunction) instance.clone();
    return eval;
  }
}