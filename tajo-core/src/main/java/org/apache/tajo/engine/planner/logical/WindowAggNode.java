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

package org.apache.tajo.engine.planner.logical;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.WindowFunctionEval;
import org.apache.tajo.engine.planner.PlanString;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.util.TUtil;

public class WindowAggNode extends UnaryNode implements Projectable, Cloneable {
	/** partition key sets */
  @Expose private Column [] partitionKeys;
  /** order key sets */
  @Expose private SortSpec [] sortSpecs;

  /** Aggregation Functions */
  @Expose private WindowFunctionEval[] windowFuncs;
  /**
   * It's a list of targets. The partition key columns should be followed by window functions.
   * aggrFunctions keep actual aggregation functions, but it only contains field references.
   * */
  @Expose private Target [] targets;
  @Expose private boolean hasDistinct = false;

  public WindowAggNode(int pid) {
    super(pid, NodeType.WINDOW_AGG);
  }

  public final boolean hasPartitionKeys() {
    return partitionKeys != null && partitionKeys.length > 0;
  }

  public void setPartitionKeys(Column[] groupingColumns) {
    this.partitionKeys = groupingColumns;
  }

	public final Column [] getPartitionKeys() {
	  return this.partitionKeys;
	}

  public final boolean hasSortSpecs() {
    return this.sortSpecs != null;
  }

  public void setSortSpecs(SortSpec [] sortSpecs) {
    this.sortSpecs = sortSpecs;
  }

  public final SortSpec [] getSortSpecs() {
    return this.sortSpecs;
  }

  public final boolean isDistinct() {
    return hasDistinct;
  }

  public void setDistinct(boolean distinct) {
    hasDistinct = distinct;
  }

  public boolean hasAggFunctions() {
    return this.windowFuncs != null;
  }

  public WindowFunctionEval [] getWindowFunctions() {
    return this.windowFuncs;
  }

  public void setWindowFunctions(WindowFunctionEval[] evals) {
    this.windowFuncs = evals;
  }

  @Override
  public boolean hasTargets() {
    return this.targets != null;
  }

  @Override
  public void setTargets(Target[] targets) {
    this.targets = targets;
    setOutSchema(PlannerUtil.targetToSchema(targets));
  }

  @Override
  public Target[] getTargets() {
    return this.targets;
  }
  
  public void setChild(LogicalNode subNode) {
    super.setChild(subNode);
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder("WinAgg (");
    if (hasPartitionKeys()) {
      sb.append("partition keys=").append(TUtil.arrayToString(partitionKeys));
      sb.append(", ");
    }
    if (hasAggFunctions()) {
      sb.append("funcs=").append(TUtil.arrayToString(windowFuncs));
    }
    if (hasSortSpecs()) {
      sb.append("sort=").append(TUtil.arrayToString(sortSpecs));
    }
    sb.append(")");
    return sb.toString();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof WindowAggNode) {
      WindowAggNode other = (WindowAggNode) obj;
      boolean eq = super.equals(other);
      eq = eq && TUtil.checkEquals(partitionKeys, other.partitionKeys);
      eq = eq && TUtil.checkEquals(sortSpecs, other.sortSpecs);
      eq = eq && TUtil.checkEquals(windowFuncs, other.windowFuncs);
      eq = eq && TUtil.checkEquals(targets, other.targets);
      eq = eq && TUtil.checkEquals(hasDistinct, other.hasDistinct);
      return eq;
    } else {
      return false;  
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(partitionKeys, sortSpecs, windowFuncs, targets, hasDistinct);
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    WindowAggNode grp = (WindowAggNode) super.clone();
    if (partitionKeys != null) {
      grp.partitionKeys = new Column[partitionKeys.length];
      for (int i = 0; i < partitionKeys.length; i++) {
        grp.partitionKeys[i] = partitionKeys[i];
      }
    }

    if (windowFuncs != null) {
      grp.windowFuncs = new WindowFunctionEval[windowFuncs.length];
      for (int i = 0; i < windowFuncs.length; i++) {
        grp.windowFuncs[i] = (WindowFunctionEval) windowFuncs[i].clone();
      }
    }

    if (targets != null) {
      grp.targets = new Target[targets.length];
      for (int i = 0; i < targets.length; i++) {
        grp.targets[i] = (Target) targets[i].clone();
      }
    }

    return grp;
  }

  @Override
  public PlanString getPlanString() {
    PlanString planStr = new PlanString(this);

    StringBuilder sb = new StringBuilder();
    sb.append("(");
    if (hasPartitionKeys()) {
      sb.append("PARTITION BY ");
      for (int j = 0; j < partitionKeys.length; j++) {
        sb.append(partitionKeys[j].getSimpleName());
        if(j < partitionKeys.length - 1) {
          sb.append(",");
        }
      }
    }

    if (hasSortSpecs()) {
      sb.append("ORDER BY ");
      for (int i = 0; i < sortSpecs.length; i++) {
        sb.append(sortSpecs[i].getSortKey().getSimpleName()).append(" ")
            .append(sortSpecs[i].isAscending() ? "asc" : "desc");
        if( i < sortSpecs.length - 1) {
          sb.append(",");
        }
      }
    }

    sb.append(")");

    planStr.appendTitle(sb.toString());

    // there can be no aggregation functions
    if (hasAggFunctions()) {
      sb = new StringBuilder();
      sb.append("(");

      for (int j = 0; j < windowFuncs.length; j++) {
        sb.append(windowFuncs[j]);
        if(j < windowFuncs.length - 1) {
          sb.append(",");
        }
      }
      sb.append(")");
      planStr.appendExplain("exprs: ").appendExplain(sb.toString());
    }

    sb = new StringBuilder("target list: ");
    for (int i = 0; i < targets.length; i++) {
      sb.append(targets[i]);
      if( i < targets.length - 1) {
        sb.append(", ");
      }
    }
    planStr.addExplan(sb.toString());

    planStr.addDetail("out schema:").appendDetail(getOutSchema().toString());
    planStr.addDetail("in schema:").appendDetail(getInSchema().toString());

    return planStr;
  }
}
