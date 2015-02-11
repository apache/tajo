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

package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.storage.FrameTuple;
import org.apache.tajo.worker.TaskAttemptContext;

public class JoinExecContext {

  private JoinNode plan;
  private EvalNode joinQual;
  private EvalNode joinFilter;

  public JoinExecContext(final JoinNode joinNode) {
    this.plan = joinNode;
    this.joinQual = joinNode.hasJoinQual() ? joinNode.getJoinQual() : null;
    this.joinFilter = joinNode.hasJoinFilter() ? joinNode.getJoinFilter() : null;
  }

  public JoinNode getPlan() {
    return plan;
  }

  public boolean hasJoinQual() {
    return this.joinQual != null;
  }

  public void setJoinQual(EvalNode joinQual) {
    this.joinQual = joinQual;
  }

  public void setPrecompiledJoinQual(TaskAttemptContext context, Schema inSchema) {
    if (hasJoinQual()) {
      joinQual = context.getPrecompiledEval(inSchema, joinQual);
    }
    if (hasJoinFilter()) {
      joinFilter = context.getPrecompiledEval(inSchema, joinFilter);
    }
  }

  public EvalNode getJoinQual() {
    return joinQual;
  }

  public boolean hasJoinFilter() {
    return this.joinFilter != null;
  }

  public void setJoinFilter(EvalNode joinFilter) {
    this.joinFilter = joinFilter;
  }

  public EvalNode getJoinFilter() {
    return joinFilter;
  }

  public boolean evalFilter(Schema inSchema, FrameTuple frameTuple) {
    return hasJoinFilter() ?
        joinFilter.eval(inSchema, frameTuple).isTrue() : true;
  }

  public boolean evalQual(Schema inSchema, FrameTuple frameTuple) {
    return hasJoinQual() ?
        joinQual.eval(inSchema, frameTuple).isTrue() : true;
  }
}
