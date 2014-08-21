/*
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

package org.apache.tajo.worker;

import com.google.common.collect.Maps;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.engine.eval.EvalNode;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExecutionBlockSharedResource {
  private AtomicBoolean initialized = new AtomicBoolean(false);
  private Map<EvalNode, EvalNode> compiledEvals = Maps.newConcurrentMap();

  public boolean initializedResources() {
    return initialized.get();
  }
}
