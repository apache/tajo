/**
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

package org.apache.tajo.engine.planner;

import org.apache.tajo.algebra.Projection;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.engine.eval.FieldEval;

import java.util.Collection;

/**
 * It manages a list of targets.
 */
public class TargetListManager {
  private final LogicalPlan plan;
  private boolean [] resolvedFlags;
  private Projection projection;
  private Target[] targets;
  private Target[] unresolvedTargets;

  public TargetListManager(LogicalPlan plan, Projection projection) {
    this.plan = plan;
    int targetNum = projection.size();
    if (projection.size() == 0) {
      resolvedFlags = new boolean[0];
    } else {
      resolvedFlags = new boolean[targetNum];
    }
    this.targets = new Target[targetNum];
    this.unresolvedTargets = new Target[targetNum];
  }

  public TargetListManager(LogicalPlan plan, Target[] unresolvedTargets) {
    this.plan = plan;

    this.targets = new Target[unresolvedTargets.length];
    this.unresolvedTargets = new Target[unresolvedTargets.length];
    for (int i = 0; i < unresolvedTargets.length; i++) {
      try {
        this.targets[i] = (Target) unresolvedTargets[i].clone();
        this.unresolvedTargets[i] = (Target) unresolvedTargets[i].clone();
      } catch (CloneNotSupportedException e) {
        e.printStackTrace();
      }
    }
    resolvedFlags = new boolean[unresolvedTargets.length];
  }

  public TargetListManager(LogicalPlan plan, String blockName) {
    this(plan, plan.getBlock(blockName).getTargetListManager().getUnresolvedTargets());
  }

  public Target getTarget(int id) {
    return targets[id];
  }

  public Target[] getTargets() {
    return this.targets;
  }

  public Target[] getUnresolvedTargets() {
    return this.unresolvedTargets;
  }

  public void update(int id, Target target) {
    this.targets[id] = target;
    this.unresolvedTargets[id] = target;
  }

  public int size() {
    return targets.length;
  }

  public void resolve(int id) {
    resolvedFlags[id] = true;
  }

  public void resolveAll() {
    for (int i = 0; i < resolvedFlags.length; i++) {
      resolvedFlags[i] = true;
    }
  }

  public boolean isEvaluated(int id) {
    return resolvedFlags[id];
  }

  public Target [] getUpdatedTarget() throws PlanningException {
    Target [] updated = new Target[targets.length];

    for (int i = 0; i < targets.length; i++) {
      if (targets[i] == null) { // if it is not created
        continue;
      }

      if (resolvedFlags[i]) { // if this target was evaluated, it becomes a column target.
        Column col = getResolvedTargetToColumn(i);
        updated[i] = new Target(new FieldEval(col));
      } else {
        try {
          updated[i] = (Target) targets[i].clone();
        } catch (CloneNotSupportedException e) {
          throw new PlanningException(e);
        }
      }
    }
    targets = updated;
    return updated;
  }

  public Schema getUpdatedSchema() {
    Schema schema = new Schema();
    for (int i = 0; i < resolvedFlags.length; i++) {
      if (resolvedFlags[i]) {
        Column col = getResolvedTargetToColumn(i);
        if (!schema.contains(col.getQualifiedName()))
        schema.addColumn(col);
      } else {
        Collection<Column> cols = getColumnRefs(i);
        for (Column col : cols) {
          if (!schema.contains(col.getQualifiedName())) {
            schema.addColumn(col);
          }
        }
      }
    }
    return schema;
  }

  public Collection<Column> getColumnRefs(int id) {
    return EvalTreeUtil.findDistinctRefColumns(targets[id].getEvalTree());
  }

  public Column getResolvedTargetToColumn(int id) {
    Target t = targets[id];
    String name;
    if (t.hasAlias() || t.getEvalTree().getType() == EvalType.FIELD) {
      name = t.getCanonicalName();
    } else { // if alias name is not given or target is an expression
      t.setAlias(plan.newNonameColumnName(t.getEvalTree().getName()));
      name = t.getCanonicalName();
    }
    return new Column(name, t.getEvalTree().getValueType()[0]);
  }

  public boolean isAllResolved() {
    for (boolean resolved : resolvedFlags) {
      if (!resolved) {
        return false;
      }
    }

    return true;
  }

}
