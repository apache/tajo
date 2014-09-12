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

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
import org.apache.tajo.engine.planner.PlanString;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.util.TUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DistinctGroupbyNode extends UnaryNode implements Projectable, Cloneable {
  @Expose
  private GroupbyNode groupbyPlan;

  @Expose
  private List<GroupbyNode> groupByNodes;

  @Expose
  private Target[] targets;

  @Expose
  private Column[] groupingColumns;

  @Expose
  private int[] resultColumnIds;

  /** Aggregation Functions */
  @Expose private AggregationFunctionCallEval [] aggrFunctions;

  public DistinctGroupbyNode(int pid) {
    super(pid, NodeType.DISTINCT_GROUP_BY);
  }

  @Override
  public boolean hasTargets() {
    return targets != null && targets.length > 0;
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

  public void setGroupbyNodes(List<GroupbyNode> groupByNodes) {
    this.groupByNodes =  groupByNodes;
  }

  public List<GroupbyNode> getGroupByNodes() {
    return groupByNodes;
  }

  public final Column[] getGroupingColumns() {
    return groupingColumns;
  }

  public final void setGroupColumns(Column[] groupingColumns) {
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

    if (groupByNodes != null) {
      cloneNode.groupByNodes = new ArrayList<GroupbyNode>();
      for (GroupbyNode eachNode: groupByNodes) {
        GroupbyNode groupbyNode = (GroupbyNode)eachNode.clone();
        groupbyNode.setPID(-1);
        cloneNode.groupByNodes.add(groupbyNode);
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
    if (groupingColumns != null || groupingColumns.length > 0) {
      sb.append("grouping set=").append(TUtil.arrayToString(groupingColumns));
      sb.append(", ");
    }
    for (GroupbyNode eachNode: groupByNodes) {
      sb.append(", groupbyNode=").append(eachNode.toString());
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DistinctGroupbyNode) {
      DistinctGroupbyNode other = (DistinctGroupbyNode) obj;
      boolean eq = super.equals(other);
      eq = eq && TUtil.checkEquals(groupingColumns, other.groupingColumns);
      eq = eq && TUtil.checkEquals(groupByNodes, other.groupByNodes);
      eq = eq && TUtil.checkEquals(targets, other.targets);
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
    for (GroupbyNode eachNode: groupByNodes) {
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

    for (GroupbyNode eachNode: groupByNodes) {
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
    for (GroupbyNode eachGroupbyNode: groupByNodes) {
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
