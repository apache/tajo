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

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.eval.FieldEval;

import java.util.Collection;

/**
 * It manages a list of targets.
 */
public class TargetListManager {
  private LogicalPlan plan;
  private boolean [] evaluatedFlags;
  private Target[] targets;
  private Target[] unevaluatedTargets;

  public TargetListManager(LogicalPlan plan, int targetNum) {
    this.plan = plan;
    if (targetNum == 0) {
      evaluatedFlags = new boolean[0];
    } else {
      evaluatedFlags = new boolean[targetNum];
    }
    this.targets = new Target[targetNum];
    this.unevaluatedTargets = new Target[targetNum];
  }

  public TargetListManager(LogicalPlan plan, Target[] original) {
    this.plan = plan;

    targets = new Target[original.length];
    unevaluatedTargets = new Target[original.length];
    for (int i = 0; i < original.length; i++) {
      try {
        targets[i] = (Target) original[i].clone();
        unevaluatedTargets[i] = (Target) original[i].clone();
      } catch (CloneNotSupportedException e) {
        e.printStackTrace();
      }
    }
    evaluatedFlags = new boolean[original.length];
  }

  public TargetListManager(LogicalPlan plan, String blockName) {
    this(plan, plan.getBlock(blockName).getTargetListManager().getUnEvaluatedTargets());
  }

  public Target getTarget(int id) {
    return targets[id];
  }

  public Target[] getTargets() {
    return this.targets;
  }

  public Target[] getUnEvaluatedTargets() {
    return this.unevaluatedTargets;
  }

  public void updateTarget(int id, Target target) {
    this.targets[id] = target;
    this.unevaluatedTargets[id] = target;
  }

  public int size() {
    return targets.length;
  }

  public void setEvaluated(int id) {
    evaluatedFlags[id] = true;
  }

  public void setEvaluatedAll() {
    for (int i = 0; i < evaluatedFlags.length; i++) {
      evaluatedFlags[i] = true;
    }
  }

  public boolean isEvaluated(int id) {
    return evaluatedFlags[id];
  }

  public Target[] getUpdatedTarget() throws PlanningException {
    Target[] updated = new Target[targets.length];
    for (int i = 0; i < targets.length; i++) {
      if (targets[i] == null) { // if it is not created
        continue;
      }

      if (evaluatedFlags[i]) { // if this target was evaluated, it becomes a column target.
        Column col = getEvaluatedColumn(i);
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
    for (int i = 0; i < evaluatedFlags.length; i++) {
      if (evaluatedFlags[i]) {
        Column col = getEvaluatedColumn(i);
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

  public Column getEvaluatedColumn(int id) {
    Target t = targets[id];
    String name;
    if (t.hasAlias()) {
      name = t.getAlias();
    } else if (t.getEvalTree().getName().equals("?")) {
      name = plan.newAnonymousColumnName();
    } else {
      name = t.getEvalTree().getName();
    }
    return new Column(name, t.getEvalTree().getValueType()[0]);
  }

  public boolean isAllEvaluated() {
    for (boolean isEval : evaluatedFlags) {
      if (!isEval) {
        return false;
      }
    }

    return true;
  }

}
