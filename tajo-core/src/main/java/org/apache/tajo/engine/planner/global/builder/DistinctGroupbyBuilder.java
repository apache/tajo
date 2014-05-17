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

package org.apache.tajo.engine.planner.global.builder;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.proto.CatalogProtos.SortSpecProto;
import org.apache.tajo.engine.eval.AggregationFunctionCallEval;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.eval.FieldEval;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.GlobalPlanner.GlobalPlanContext;
import org.apache.tajo.engine.planner.logical.DistinctGroupbyNode;
import org.apache.tajo.engine.planner.logical.GroupbyNode;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.ipc.TajoWorkerProtocol.DistinctGroupbyEnforcer.DistinctAggregationAlgorithm;
import org.apache.tajo.ipc.TajoWorkerProtocol.DistinctGroupbyEnforcer.SortSpecArray;
import org.apache.tajo.util.TUtil;

import java.util.*;

import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleType.HASH_SHUFFLE;

public class DistinctGroupbyBuilder {
  private static Log LOG = LogFactory.getLog(DistinctGroupbyBuilder.class);
  private GlobalPlanner globalPlanner;

  public DistinctGroupbyBuilder(GlobalPlanner globalPlanner) {
    this.globalPlanner = globalPlanner;
  }


  public ExecutionBlock buildPlan(GlobalPlanContext context,
                                  ExecutionBlock latestExecBlock,
                                  LogicalNode currentNode) throws PlanningException {
    try {
      GroupbyNode groupbyNode = (GroupbyNode)currentNode;
      LogicalPlan plan = context.getPlan().getLogicalPlan();
      DistinctGroupbyNode baseDistinctNode = buildBaseDistinctGroupByNode(context, latestExecBlock, groupbyNode);

      // Create First, SecondStage's Node using baseNode
      DistinctGroupbyNode[] distinctNodes = createMultiPhaseDistinctNode(plan, groupbyNode, baseDistinctNode);

      DistinctGroupbyNode firstStageDistinctNode = distinctNodes[0];
      DistinctGroupbyNode secondStageDistinctNode = distinctNodes[1];

      // Set latestExecBlock's plan with firstDistinctNode
      latestExecBlock.setPlan(firstStageDistinctNode);

      // Make SecondStage ExecutionBlock
      ExecutionBlock secondStageBlock = context.getPlan().newExecutionBlock();

      // Set Enforcer: SecondStage => SortAggregationAlgorithm
      setDistinctAggregationEnforcer(latestExecBlock, firstStageDistinctNode, secondStageBlock, secondStageDistinctNode);

      //Create data channel FirstStage to SecondStage
      DataChannel channel;
      if (groupbyNode.isEmptyGrouping()) {
        channel = new DataChannel(latestExecBlock, secondStageBlock, HASH_SHUFFLE, 1);
        channel.setShuffleKeys(firstStageDistinctNode.getGroupingColumns());
      } else {
        channel = new DataChannel(latestExecBlock, secondStageBlock, HASH_SHUFFLE, 32);
        channel.setShuffleKeys(firstStageDistinctNode.getGroupingColumns());
      }
      channel.setSchema(firstStageDistinctNode.getOutSchema());
      channel.setStoreType(globalPlanner.getStoreType());

      ScanNode scanNode = GlobalPlanner.buildInputExecutor(context.getPlan().getLogicalPlan(), channel);
      secondStageDistinctNode.setChild(scanNode);

      secondStageBlock.setPlan(secondStageDistinctNode);

      context.getPlan().addConnect(channel);

      if (GlobalPlanner.hasUnionChild(firstStageDistinctNode)) {
        globalPlanner.buildDistinctGroupbyAndUnionPlan(
            context.getPlan(), latestExecBlock, firstStageDistinctNode, firstStageDistinctNode);
      }

      return secondStageBlock;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new PlanningException(e);
    }
  }

  private DistinctGroupbyNode buildBaseDistinctGroupByNode(GlobalPlanContext context,
                                                           ExecutionBlock latestExecBlock,
                                                           GroupbyNode groupbyNode) {

    /*
     Making DistinctGroupbyNode from GroupByNode
     select col1, count(distinct col2), count(distinct col3), sum(col4) from ... group by col1
     => DistinctGroupbyNode
        grouping key = col1
        Sub GroupbyNodes
         - GroupByNode1: grouping(col1, col2), expr(count distinct col2)
         - GroupByNode2: grouping(col1, col3), expr(count distinct col3)
         - GroupByNode3: grouping(col1), expr(sum col4)
    */
    List<Column> originalGroupingColumns = Arrays.asList(groupbyNode.getGroupingColumns());

    List<GroupbyNode> childGroupbyNodes = new ArrayList<GroupbyNode>();

    List<AggregationFunctionCallEval> otherAggregationFunctionCallEvals = new ArrayList<AggregationFunctionCallEval>();
    List<Target> otherAggregationFunctionTargets = new ArrayList<Target>();

    //distinct columns -> GroupbyNode
    Map<String, DistinctGroupbyNodeBuildInfo> distinctNodeBuildInfos = new HashMap<String, DistinctGroupbyNodeBuildInfo>();

    AggregationFunctionCallEval[] aggFunctions = groupbyNode.getAggFunctions();
    for (int aggIdx = 0; aggIdx < aggFunctions.length; aggIdx++) {
      AggregationFunctionCallEval aggFunction = aggFunctions[aggIdx];
      Target aggFunctionTarget = groupbyNode.getTargets()[originalGroupingColumns.size() + aggIdx];

      if (aggFunction.isDistinct()) {
        // Create or reuse Groupby node for each Distinct expression.
        LinkedHashSet<Column> groupbyUniqColumns = EvalTreeUtil.findUniqueColumns(aggFunction);
        String groupbyMapKey = EvalTreeUtil.columnsToStr(groupbyUniqColumns);
        DistinctGroupbyNodeBuildInfo buildInfo = distinctNodeBuildInfos.get(groupbyMapKey);
        if (buildInfo == null) {
          GroupbyNode distinctGroupbyNode = new GroupbyNode(context.getPlan().getLogicalPlan().newPID());
          buildInfo = new DistinctGroupbyNodeBuildInfo(distinctGroupbyNode);
          distinctNodeBuildInfos.put(groupbyMapKey, buildInfo);

          // Grouping columns are GROUP BY clause's column + Distinct column.
          List<Column> groupingColumns = new ArrayList<Column>(originalGroupingColumns);
          for (Column eachGroupingColumn: groupbyUniqColumns) {
            if (!groupingColumns.contains(eachGroupingColumn)) {
              groupingColumns.add(eachGroupingColumn);
            }
          }
          distinctGroupbyNode.setGroupingColumns(groupingColumns.toArray(new Column[]{}));
        }
        buildInfo.addAggFunction(aggFunction);
        buildInfo.addAggFunctionTarget(aggFunctionTarget);
      } else {
        otherAggregationFunctionCallEvals.add(aggFunction);
        otherAggregationFunctionTargets.add(aggFunctionTarget);
      }
    }

    //Add child groupby node for each Distinct clause
    for (String eachKey: distinctNodeBuildInfos.keySet()) {
      DistinctGroupbyNodeBuildInfo buildInfo = distinctNodeBuildInfos.get(eachKey);
      GroupbyNode eachGroupbyNode = buildInfo.getGroupbyNode();
      List<AggregationFunctionCallEval> groupbyAggFunctions = buildInfo.getAggFunctions();
      Target[] targets = new Target[eachGroupbyNode.getGroupingColumns().length + groupbyAggFunctions.size()];
      int targetIdx = 0;

      for (Column column : eachGroupbyNode.getGroupingColumns()) {
        Target target = new Target(new FieldEval(column));
        targets[targetIdx++] = target;
      }
      for (Target eachAggFunctionTarget: buildInfo.getAggFunctionTargets()) {
        targets[targetIdx++] = eachAggFunctionTarget;
      }
      eachGroupbyNode.setTargets(targets);
      eachGroupbyNode.setAggFunctions(groupbyAggFunctions.toArray(new AggregationFunctionCallEval[]{}));
      eachGroupbyNode.setDistinct(true);
      eachGroupbyNode.setInSchema(groupbyNode.getInSchema());

      childGroupbyNodes.add(eachGroupbyNode);
    }

    // Merge other aggregation function to a GroupBy Node.
    if (!otherAggregationFunctionCallEvals.isEmpty()) {
      // finally this aggregation output tuple's order is GROUP_BY_COL1, COL2, .... + AGG_VALUE, SUM_VALUE, ...
      GroupbyNode otherGroupbyNode = new GroupbyNode(context.getPlan().getLogicalPlan().newPID());

      Target[] targets = new Target[originalGroupingColumns.size() + otherAggregationFunctionTargets.size()];
      int targetIdx = 0;
      for (Column column : originalGroupingColumns) {
        Target target = new Target(new FieldEval(column));
        targets[targetIdx++] = target;
      }
      for (Target eachTarget : otherAggregationFunctionTargets) {
        targets[targetIdx++] = eachTarget;
      }

      otherGroupbyNode.setTargets(targets);
      otherGroupbyNode.setGroupingColumns(originalGroupingColumns.toArray(new Column[]{}));
      otherGroupbyNode.setAggFunctions(otherAggregationFunctionCallEvals.toArray(new AggregationFunctionCallEval[]{}));
      otherGroupbyNode.setInSchema(groupbyNode.getInSchema());

      childGroupbyNodes.add(otherGroupbyNode);
    }

    DistinctGroupbyNode baseDistinctNode = new DistinctGroupbyNode(context.getPlan().getLogicalPlan().newPID());
    baseDistinctNode.setTargets(groupbyNode.getTargets());
    baseDistinctNode.setGroupColumns(groupbyNode.getGroupingColumns());
    baseDistinctNode.setInSchema(groupbyNode.getInSchema());
    baseDistinctNode.setChild(groupbyNode.getChild());

    baseDistinctNode.setGroupbyNodes(childGroupbyNodes);

    return baseDistinctNode;
  }

  public DistinctGroupbyNode[] createMultiPhaseDistinctNode(LogicalPlan plan,
                                                                   GroupbyNode originGroupbyNode,
                                                                   DistinctGroupbyNode baseDistinctNode) {
    /*
    Creating 2 stage execution block
      - first stage: HashAggregation -> groupby distinct column and eval not distinct aggregation
        ==> HashShuffle
      - second stage: SortAggregation -> sort and eval(aggregate) with distinct aggregation function, not distinct aggregation

    select col1, count(distinct col2), count(distinct col3), sum(col4) from ... group by col1
    -------------------------------------------------------------------------
    - baseDistinctNode
      grouping key = col1
      - GroupByNode1: grouping(col1, col2), expr(count distinct col2)
      - GroupByNode2: grouping(col1, col3), expr(count distinct col3)
      - GroupByNode3: grouping(col1), expr(sum col4)
    -------------------------------------------------------------------------
    - FirstStage:
      - GroupByNode1: grouping(col1, col2)
      - GroupByNode2: grouping(col1, col3)
      - GroupByNode3: grouping(col1), expr(sum col4)

    - SecondStage:
      - GroupByNode1: grouping(col1, col2), expr(count distinct col2)
      - GroupByNode2: grouping(col1, col3), expr(count distinct col3)
      - GroupByNode3: grouping(col1), expr(sum col4)
    */

    Preconditions.checkNotNull(baseDistinctNode);

    Schema originOutputSchema = originGroupbyNode.getOutSchema();
    DistinctGroupbyNode firstStageDistinctNode = PlannerUtil.clone(plan, baseDistinctNode);
    DistinctGroupbyNode secondStageDistinctNode = baseDistinctNode;

    List<Column> originGroupColumns = Arrays.asList(firstStageDistinctNode.getGroupingColumns());

    int[] secondStageColumnIds = new int[secondStageDistinctNode.getOutSchema().size()];
    int columnIdIndex = 0;
    for (Column column: secondStageDistinctNode.getGroupingColumns()) {
      secondStageColumnIds[originOutputSchema.getColumnId(column.getQualifiedName())] = columnIdIndex;
      columnIdIndex++;
    }

    // Split groupby node into two stage.
    // - Remove distinct aggregations from FirstStage.
    // - Change SecondStage's aggregation expr and target column name. For example:
    //     exprs: (sum(default.lineitem.l_quantity (FLOAT8))) ==> exprs: (sum(?sum_3 (FLOAT8)))
    int grpIdx = 0;
    for (GroupbyNode firstStageGroupbyNode: firstStageDistinctNode.getGroupByNodes()) {
      GroupbyNode secondStageGroupbyNode = secondStageDistinctNode.getGroupByNodes().get(grpIdx);

      if (firstStageGroupbyNode.isDistinct()) {
        // FirstStage: Remove aggregation, Set target with only grouping columns
        firstStageGroupbyNode.setAggFunctions(null);

        List<Target> firstGroupbyTargets = new ArrayList<Target>();
        for (Column column : firstStageGroupbyNode.getGroupingColumns()) {
          Target target = new Target(new FieldEval(column));
          firstGroupbyTargets.add(target);
        }
        firstStageGroupbyNode.setTargets(firstGroupbyTargets.toArray(new Target[]{}));

        // SecondStage:
        //   Set grouping column with origin groupby's columns
        //   Remove distinct group column from targets
        secondStageGroupbyNode.setGroupingColumns(originGroupColumns.toArray(new Column[]{}));

        Target[] oldTargets = secondStageGroupbyNode.getTargets();
        List<Target> secondGroupbyTargets = new ArrayList<Target>();
        LinkedHashSet<Column> distinctColumns = EvalTreeUtil.findUniqueColumns(secondStageGroupbyNode.getAggFunctions()[0]);
        List<Column> uniqueDistinctColumn = new ArrayList<Column>();
        // remove origin group by column from distinctColumns
        for (Column eachColumn: distinctColumns) {
          if (!originGroupColumns.contains(eachColumn)) {
            uniqueDistinctColumn.add(eachColumn);
          }
        }
        for (int i = 0; i < originGroupColumns.size(); i++) {
          secondGroupbyTargets.add(oldTargets[i]);
          if (grpIdx > 0) {
            columnIdIndex++;
          }
        }

        for (int aggFuncIdx = 0; aggFuncIdx < secondStageGroupbyNode.getAggFunctions().length; aggFuncIdx++) {
          int targetIdx = originGroupColumns.size() + uniqueDistinctColumn.size() + aggFuncIdx;
          Target aggFuncTarget = oldTargets[targetIdx];
          secondGroupbyTargets.add(aggFuncTarget);
          int outputColumnId = originOutputSchema.getColumnId(aggFuncTarget.getNamedColumn().getQualifiedName());
          secondStageColumnIds[outputColumnId] = columnIdIndex;
          columnIdIndex++;
        }
        secondStageGroupbyNode.setTargets(secondGroupbyTargets.toArray(new Target[]{}));
      } else {
        // FirstStage: Change target of aggFunction to function name expr
        List<Target> firstGroupbyTargets = new ArrayList<Target>();
        for (Column column : firstStageDistinctNode.getGroupingColumns()) {
          firstGroupbyTargets.add(new Target(new FieldEval(column)));
          columnIdIndex++;
        }

        int aggFuncIdx = 0;
        for (AggregationFunctionCallEval aggFunction: firstStageGroupbyNode.getAggFunctions()) {
          aggFunction.setFirstPhase();
          String firstEvalNames = plan.generateUniqueColumnName(aggFunction);
          FieldEval firstEval = new FieldEval(firstEvalNames, aggFunction.getValueType());
          firstGroupbyTargets.add(new Target(firstEval));

          AggregationFunctionCallEval secondStageAggFunction = secondStageGroupbyNode.getAggFunctions()[aggFuncIdx];
          secondStageAggFunction.setArgs(new EvalNode[] {firstEval});

          Target secondTarget = secondStageGroupbyNode.getTargets()[secondStageGroupbyNode.getGroupingColumns().length + aggFuncIdx];
          int outputColumnId = originOutputSchema.getColumnId(secondTarget.getNamedColumn().getQualifiedName());
          secondStageColumnIds[outputColumnId] = columnIdIndex;
          columnIdIndex++;
          aggFuncIdx++;
        }
        firstStageGroupbyNode.setTargets(firstGroupbyTargets.toArray(new Target[]{}));
        secondStageGroupbyNode.setInSchema(firstStageGroupbyNode.getOutSchema());
      }
      grpIdx++;
    }

    // In the case of distinct query without group by clause
    // other aggregation function is added to last distinct group by node.
    List<GroupbyNode> secondStageGroupbyNodes = secondStageDistinctNode.getGroupByNodes();
    GroupbyNode lastSecondStageGroupbyNode = secondStageGroupbyNodes.get(secondStageGroupbyNodes.size() - 1);
    if (!lastSecondStageGroupbyNode.isDistinct() && lastSecondStageGroupbyNode.isEmptyGrouping()) {
      GroupbyNode otherGroupbyNode = lastSecondStageGroupbyNode;
      lastSecondStageGroupbyNode = secondStageGroupbyNodes.get(secondStageGroupbyNodes.size() - 2);
      secondStageGroupbyNodes.remove(secondStageGroupbyNodes.size() - 1);

      Target[] targets =
          new Target[lastSecondStageGroupbyNode.getTargets().length + otherGroupbyNode.getTargets().length];
      System.arraycopy(lastSecondStageGroupbyNode.getTargets(), 0,
          targets, 0, lastSecondStageGroupbyNode.getTargets().length);
      System.arraycopy(otherGroupbyNode.getTargets(), 0, targets,
          lastSecondStageGroupbyNode.getTargets().length, otherGroupbyNode.getTargets().length);

      lastSecondStageGroupbyNode.setTargets(targets);

      AggregationFunctionCallEval[] aggFunctions =
          new AggregationFunctionCallEval[lastSecondStageGroupbyNode.getAggFunctions().length + otherGroupbyNode.getAggFunctions().length];
      System.arraycopy(lastSecondStageGroupbyNode.getAggFunctions(), 0,
          aggFunctions, 0, lastSecondStageGroupbyNode.getAggFunctions().length);
      System.arraycopy(otherGroupbyNode.getAggFunctions(), 0, aggFunctions,
          lastSecondStageGroupbyNode.getAggFunctions().length, otherGroupbyNode.getAggFunctions().length);

      lastSecondStageGroupbyNode.setAggFunctions(aggFunctions);
    }

    // Set FirstStage DistinctNode's target with grouping column and other aggregation function
    List<Integer> firstStageColumnIds = new ArrayList<Integer>();
    columnIdIndex = 0;
    List<Target> firstTargets = new ArrayList<Target>();
    for (GroupbyNode firstStageGroupbyNode: firstStageDistinctNode.getGroupByNodes()) {
      if (firstStageGroupbyNode.isDistinct()) {
        for (Column column : firstStageGroupbyNode.getGroupingColumns()) {
          Target firstTarget = new Target(new FieldEval(column));
          if (!firstTargets.contains(firstTarget)) {
            firstTargets.add(firstTarget);
            firstStageColumnIds.add(columnIdIndex);
          }
          columnIdIndex++;
        }
      } else {
        //add aggr function target
        columnIdIndex += firstStageGroupbyNode.getGroupingColumns().length;
        Target[] baseGroupbyTargets = firstStageGroupbyNode.getTargets();
        for (int i = firstStageGroupbyNode.getGroupingColumns().length;
             i < baseGroupbyTargets.length; i++) {
          firstTargets.add(baseGroupbyTargets[i]);
          firstStageColumnIds.add(columnIdIndex++);
        }
      }
    }
    firstStageDistinctNode.setTargets(firstTargets.toArray(new Target[]{}));
    firstStageDistinctNode.setResultColumnIds(TUtil.toArray(firstStageColumnIds));

    //Set SecondStage ColumnId and Input schema
    secondStageDistinctNode.setResultColumnIds(secondStageColumnIds);

    Schema secondStageInSchema = new Schema();
    //TODO merged tuple schema
    int index = 0;
    for(GroupbyNode eachNode: secondStageDistinctNode.getGroupByNodes()) {
      eachNode.setInSchema(firstStageDistinctNode.getOutSchema());
      for (Column column: eachNode.getOutSchema().getColumns()) {
        if (secondStageInSchema.getColumn(column) == null) {
          secondStageInSchema.addColumn(column);
        }
      }
    }
    secondStageDistinctNode.setInSchema(secondStageInSchema);

    return new DistinctGroupbyNode[]{firstStageDistinctNode, secondStageDistinctNode};
  }

  private void setDistinctAggregationEnforcer(
      ExecutionBlock firstStageBlock, DistinctGroupbyNode firstStageDistinctNode,
      ExecutionBlock secondStageBlock, DistinctGroupbyNode secondStageDistinctNode) {
    firstStageBlock.getEnforcer().enforceDistinctAggregation(firstStageDistinctNode.getPID(),
        DistinctAggregationAlgorithm.HASH_AGGREGATION, null);

    List<SortSpecArray> sortSpecArrays = new ArrayList<SortSpecArray>();
    int index = 0;
    for (GroupbyNode groupbyNode: firstStageDistinctNode.getGroupByNodes()) {
      List<SortSpecProto> sortSpecs = new ArrayList<SortSpecProto>();
      for (Column column: groupbyNode.getGroupingColumns()) {
        sortSpecs.add(SortSpecProto.newBuilder().setColumn(column.getProto()).build());
      }
      sortSpecArrays.add( SortSpecArray.newBuilder()
          .setPid(secondStageDistinctNode.getGroupByNodes().get(index).getPID())
          .addAllSortSpecs(sortSpecs).build());
    }
    secondStageBlock.getEnforcer().enforceDistinctAggregation(secondStageDistinctNode.getPID(),
        DistinctAggregationAlgorithm.SORT_AGGREGATION, sortSpecArrays);

  }

  static class DistinctGroupbyNodeBuildInfo {
    private GroupbyNode groupbyNode;
    private List<AggregationFunctionCallEval> aggFunctions = new ArrayList<AggregationFunctionCallEval>();
    private List<Target> aggFunctionTargets = new ArrayList<Target>();

    public DistinctGroupbyNodeBuildInfo(GroupbyNode groupbyNode) {
      this.groupbyNode = groupbyNode;
    }

    public GroupbyNode getGroupbyNode() {
      return groupbyNode;
    }

    public List<AggregationFunctionCallEval> getAggFunctions() {
      return aggFunctions;
    }

    public List<Target> getAggFunctionTargets() {
      return aggFunctionTargets;
    }

    public void addAggFunction(AggregationFunctionCallEval aggFunction) {
      this.aggFunctions.add(aggFunction);
    }

    public void addAggFunctionTarget(Target target) {
      this.aggFunctionTargets.add(target);
    }
  }
}
