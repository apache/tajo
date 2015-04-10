/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.function;

import com.google.common.base.Objects;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.plan.expr.FunctionEval;
import org.apache.tajo.plan.function.python.ScriptExecutor;

import java.util.Arrays;

/**
 * This class contains some metadata need to execute functions.
 */
public class FunctionInvokeContext {
  private final OverridableConf queryContext;
  private final FunctionEval.ParamType[] paramTypes;
  private ScriptExecutor scriptExecutor;

  public FunctionInvokeContext(OverridableConf queryContext, FunctionEval.ParamType[] paramTypes) {
    this.queryContext = queryContext;
    this.paramTypes = paramTypes;
  }

  public OverridableConf getQueryContext() {
    return queryContext;
  }

  public FunctionEval.ParamType[] getParamTypes() {
    return paramTypes;
  }

  public void setScriptExecutor(ScriptExecutor scriptExecutor) {
    this.scriptExecutor = scriptExecutor;
  }

  public boolean hasScriptExecutor() {
    return scriptExecutor != null;
  }

  public ScriptExecutor getScriptExecutor() {
    return scriptExecutor;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(queryContext, Arrays.hashCode(paramTypes));
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof FunctionInvokeContext) {
      FunctionInvokeContext other = (FunctionInvokeContext) o;
      return queryContext.equals(other.queryContext) && Arrays.equals(paramTypes, other.paramTypes);
    }
    return false;
  }
}
