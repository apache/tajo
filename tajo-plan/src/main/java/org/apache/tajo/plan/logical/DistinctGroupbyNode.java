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

package org.apache.tajo.plan.logical;

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.plan.PlanString;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.AggregationFunctionCallEval;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.util.TUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DistinctGroupbyNode extends UnaryNode implements Projectable, Cloneable {
  @Expose
  private GroupbyNode groupbyPlan;

  @Expose
  private List<GroupbyNode> subGroupbyPlan;

  @Expose
  private Target[] targets;

  @Expose
  private Column[] groupingColumns = PlannerUtil.EMPTY_COLUMNS;

  @Expose
  private int[] resultColumnIds = new int[]{};

  /** Aggregation Functions */
  @Expose private AggregationFunctionCallEval[] aggrFunctions = PlannerUtil.EMPTY_AGG_FUNCS;

  public DistinctGroupbyNode(int pid) {
    super(pid, NodeType.DISTINCT_GROUP_BY);
  }

  @Override
  public boolean hasTargets() {
    return targets.length > 0;
  }

  @Override
  public void setTargets(Target[] targets) {
    this.targets = targets;
    setOutSchema(PlannerUtil.targetToSchema(targets));
  }

  @Override
  public Target[] getTargets() {
    if (hasTargets()) {
      return targets;
    } else {
      return new Target[0];
    }
  }

  public void setSubPlans(List<GroupbyNode> groupByNodes) {
    this.subGroupbyPlan =  groupByNodes;
  }

  public List<GroupbyNode> getSubPlans() {
    return subGroupbyPlan;
  }

  public final Column[] getGroupingColumns() {
    return groupingColumns;
  }

  public final void setGroupingColumns(Column[] groupingColumns) {
    this.groupingColumns = groupingColumns;
  }

  public int[] getResultColumnIds() {
    return resultColumnIds;
  }

  public void setResultColumnIds(int[] resultColumnIds) {
    this.resultColumnIds = resultColumnIds;
  }

  public AggregationFunctionCallEval [] getAggFunctions() {
    return this.aggrFunctions;
  }

  public void setAggFunctions(AggregationFunctionCallEval[] evals) {
    this.aggrFunctions = evals;
  }

  public void setGroupbyPlan(GroupbyNode groupbyPlan) { this.groupbyPlan = groupbyPlan; }

  public GroupbyNode getGroupbyPlan() { return this.groupbyPlan; }

  @Override
  public Object clone() throws CloneNotSupportedException {
    DistinctGroupbyNode cloneNode = (DistinctGroupbyNode)super.clone();

    if (groupingColumns != null) {
      cloneNode.groupingColumns = new Column[groupingColumns.length];
      for (int i = 0; i < groupingColumns.length; i++) {
        cloneNode.groupingColumns[i] = groupingColumns[i];
      }
    }

    if (subGroupbyPlan != null) {
      cloneNode.subGroupbyPlan = new ArrayList<GroupbyNode>();
      for (GroupbyNode eachNode: subGroupbyPlan) {
        GroupbyNode groupbyNode = (GroupbyNode)eachNode.clone();
        groupbyNode.setPID(-1);
        cloneNode.subGroupbyPlan.add(groupbyNode);
      }
    }

    if (targets != null) {
      cloneNode.targets = new Target[targets.length];
      for (int i = 0; i < targets.length; i++) {
        cloneNode.targets[i] = (Target) targets[i].clone();
      }
    }

    if (groupbyPlan != null) {
      cloneNode.groupbyPlan = (GroupbyNode)groupbyPlan.clone();
    }
    return cloneNode;
  }

  public final boolean isEmptyGrouping() {
    return groupingColumns == null || groupingColumns.length == 0;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("Distinct GroupBy (");
    if (groupingColumns != null && groupingColumns.length > 0) {
      sb.append("grouping set=").append(StringUtils.join(groupingColumns));
      sb.append(", ");
    }
    for (GroupbyNode eachNode: subGroupbyPlan) {
      sb.append(", groupbyNode=").append(eachNode.toString());
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(aggrFunctions);
    result = prime * result + ((groupbyPlan == null) ? 0 : groupbyPlan.hashCode());
    result = prime * result + Arrays.hashCode(groupingColumns);
    result = prime * result + Arrays.hashCode(resultColumnIds);
    result = prime * result + ((subGroupbyPlan == null) ? 0 : subGroupbyPlan.hashCode());
    result = prime * result + Arrays.hashCode(targets);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DistinctGroupbyNode) {
      DistinctGroupbyNode other = (DistinctGroupbyNode) obj;
      boolean eq = super.equals(other);
      eq = eq && TUtil.checkEquals(groupingColumns, other.groupingColumns);
      eq = eq && TUtil.checkEquals(subGroupbyPlan, other.subGroupbyPlan);
      eq = eq && TUtil.checkEquals(targets, other.targets);
      eq = eq && TUtil.checkEquals(resultColumnIds, other.resultColumnIds);
      return eq;
    } else {
      return false;
    }
  }

  @Override
  public PlanString getPlanString() {
    PlanString planStr = new PlanString(this);

    StringBuilder sb = new StringBuilder();
    sb.append("(");
    Column [] groupingColumns = this.groupingColumns;
    for (int j = 0; j < groupingColumns.length; j++) {
      sb.append(groupingColumns[j].getSimpleName());
      if(j < groupingColumns.length - 1) {
        sb.append(",");
      }
    }

    sb.append(")");

    planStr.appendTitle(sb.toString());

    sb = new StringBuilder();
    sb.append("(");

    String prefix = "";
    for (GroupbyNode eachNode: subGroupbyPlan) {
      if (eachNode.hasAggFunctions()) {
        AggregationFunctionCallEval[] aggrFunctions = eachNode.getAggFunctions();
        for (int j = 0; j < aggrFunctions.length; j++) {
          sb.append(prefix).append(aggrFunctions[j]);
          prefix = ",";
        }
      }
    }
    sb.append(")");
    planStr.appendExplain("exprs: ").appendExplain(sb.toString());

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

    for (GroupbyNode eachNode: subGroupbyPlan) {
      planStr.addDetail("\t").appendDetail("distinct: " + eachNode.isDistinct())
          .appendDetail(", " + eachNode.getShortPlanString());
    }

    return planStr;
  }

  public Column[] getFirstStageShuffleKeyColumns() {
    List<Column> shuffleKeyColumns = new ArrayList<Column>();
    shuffleKeyColumns.add(getOutSchema().getColumn(0));   //distinctseq column
    if (groupingColumns != null) {
      for (Column eachColumn: groupingColumns) {
        if (!shuffleKeyColumns.contains(eachColumn)) {
          shuffleKeyColumns.add(eachColumn);
        }
      }
    }
    for (GroupbyNode eachGroupbyNode: subGroupbyPlan) {
      if (eachGroupbyNode.getGroupingColumns() != null && eachGroupbyNode.getGroupingColumns().length > 0) {
        for (Column eachColumn: eachGroupbyNode.getGroupingColumns()) {
          if (!shuffleKeyColumns.contains(eachColumn)) {
            shuffleKeyColumns.add(eachColumn);
          }
        }
      }
    }

    return shuffleKeyColumns.toArray(new Column[]{});
  }
}
