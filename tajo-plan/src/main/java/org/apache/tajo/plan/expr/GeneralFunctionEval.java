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
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.TUtil;

import javax.annotation.Nullable;
import java.io.IOException;

public class GeneralFunctionEval extends FunctionEval {
  @Expose protected FunctionInvoke funcInvoke;
  @Expose protected OverridableConf queryContext;

	public GeneralFunctionEval(@Nullable OverridableConf queryContext, FunctionDesc desc, EvalNode[] givenArgs)
      throws IOException {
		super(EvalType.FUNCTION, desc, givenArgs);
    this.queryContext = queryContext;
  }

  @Override
  public EvalNode bind(Schema schema) {
    super.bind(schema);
    try {
      this.funcInvoke = FunctionInvoke.newInstance(funcDesc);
      this.funcInvoke.init(queryContext, getParamType());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Datum eval(Tuple tuple) {
    super.eval(tuple);
    Datum res = funcInvoke.eval(evalParams(tuple));
    funcInvoke.close();
    return res;
  }

	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof GeneralFunctionEval) {
      GeneralFunctionEval other = (GeneralFunctionEval) obj;
      return super.equals(other) &&
          TUtil.checkEquals(funcInvoke, other.funcInvoke);
	  }
	  
	  return false;
	}
	
	@Override
	public int hashCode() {
	  return Objects.hashCode(funcDesc, funcInvoke);
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
    GeneralFunctionEval eval = (GeneralFunctionEval) super.clone();
    if (funcInvoke != null) {
      eval.funcInvoke = (FunctionInvoke) funcInvoke.clone();
    }
    return eval;
  }
}