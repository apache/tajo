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

import org.apache.tajo.plan.function.python.ScriptExecutor;
import org.apache.tajo.util.TUtil;

import java.util.Collection;
import java.util.Map;

public class EvalContext {
  private final Map<EvalNode, ScriptExecutor> scriptExecutorMap = TUtil.newHashMap();

  public void addScriptExecutor(EvalNode evalNode, ScriptExecutor scriptExecutor) {
    this.scriptExecutorMap.put(evalNode, scriptExecutor);
  }

  public boolean hasScriptExecutor(EvalNode evalNode) {
    return this.scriptExecutorMap.containsKey(evalNode);
  }

  public ScriptExecutor getScriptExecutor(EvalNode evalNode) {
    return this.scriptExecutorMap.get(evalNode);
  }

  public Collection<ScriptExecutor> getAllScriptExecutors() {
    return this.scriptExecutorMap.values();
  }
}
