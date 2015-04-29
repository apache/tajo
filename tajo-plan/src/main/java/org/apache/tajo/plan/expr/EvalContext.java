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

import org.apache.tajo.plan.function.python.TajoScriptEngine;
import org.apache.tajo.util.TUtil;

import java.util.Collection;
import java.util.Map;

public class EvalContext {
  private final Map<EvalNode, TajoScriptEngine> scriptEngineMap = TUtil.newHashMap();

  public void addScriptEngine(EvalNode evalNode, TajoScriptEngine scriptExecutor) {
    this.scriptEngineMap.put(evalNode, scriptExecutor);
  }

  public boolean hasScriptEngine(EvalNode evalNode) {
    boolean contain = this.scriptEngineMap.containsKey(evalNode);
    return contain;
  }

  public TajoScriptEngine getScriptEngine(EvalNode evalNode) {
    return this.scriptEngineMap.get(evalNode);
  }

  public Collection<TajoScriptEngine> getAllScriptEngines() {
    return this.scriptEngineMap.values();
  }
}
