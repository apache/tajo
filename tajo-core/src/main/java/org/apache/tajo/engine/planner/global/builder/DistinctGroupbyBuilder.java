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
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.engine.planner.global.DataChannel;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.GlobalPlanner;
import org.apache.tajo.engine.planner.global.GlobalPlanner.GlobalPlanContext;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.plan.serder.PlanProto.DistinctGroupbyEnforcer.DistinctAggregationAlgorithm;
import org.apache.tajo.plan.serder.PlanProto.DistinctGroupbyEnforcer.MultipleAggregationStage;
import org.apache.tajo.plan.serder.PlanProto.DistinctGroupbyEnforcer.SortSpecArray;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.expr.AggregationFunctionCallEval;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.EvalTreeUtil;
import org.apache.tajo.plan.expr.FieldEval;
import org.apache.tajo.plan.logical.DistinctGroupbyNode;
import org.apache.tajo.plan.logical.GroupbyNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.util.TUtil;

import java.util.*;

import static org.apache.tajo.plan.serder.PlanProto.ShuffleType.HASH_SHUFFLE;

public class DistinctGroupbyBuilder {
  private static Log LOG = LogFactory.getLog(DistinctGroupbyBuilder.class);
  private GlobalPlanner globalPlanner;

  public DistinctGroupbyBuilder(GlobalPlanner globalPlanner) {
    this.globalPlanner = globalPlanner;
  }

  public ExecutionBlock buildMultiLevelPlan(GlobalPlanContext context,
                                            ExecutionBlock latestExecBlock,
                                            LogicalNode currentNode) {
    try {
      GroupbyNode groupbyNode = (GroupbyNode) currentNode;

      LogicalPlan plan = context.getPlan().getLogicalPlan();

      DistinctGroupbyNode baseDistinctNode =
          buildMultiLevelBaseDistinctGroupByNode(context, latestExecBlock, groupbyNode);
      baseDistinctNode.setGroupbyPlan(groupbyNode);

      // Set total Aggregation Functions.
      List<AggregationFunctionCallEval> aggFunctions = new ArrayList<>();

      for (int i = 0; i < groupbyNode.getAggFunctions().size(); i++) {
        aggFunctions.add((AggregationFunctionCallEval) groupbyNode.getAggFunctions().get(i).clone());
        aggFunctions.get(i).setFirstPhase();
        // If there is not grouping column, we can't find column alias.
        // Thus we should find the alias at Groupbynode output schema.
        if (groupbyNode.getGroupingColumns().length == 0
            && aggFunctions.size() == groupbyNode.getOutSchema().getRootColumns().size()) {
          aggFunctions.get(i).setAlias(groupbyNode.getOutSchema().getColumn(i).getQualifiedName());
        }
      }

      if (groupbyNode.getGroupingColumns().length == 0
          && aggFunctions.size() == groupbyNode.getOutSchema().getRootColumns().size()) {
        groupbyNode.setAggFunctions(aggFunctions);
      }

      baseDistinctNode.setAggFunctions(aggFunctions);

      // Create First, SecondStage's Node using baseNode
      DistinctGroupbyNode firstStageDistinctNode = PlannerUtil.clone(plan, baseDistinctNode);
      DistinctGroupbyNode secondStageDistinctNode = PlannerUtil.clone(plan, baseDistinctNode);
      DistinctGroupbyNode thirdStageDistinctNode = PlannerUtil.clone(plan, baseDistinctNode);

      // Set second, third non-distinct aggregation's eval node to field eval
      GroupbyNode lastGroupbyNode = secondStageDistinctNode.getSubPlans().get(secondStageDistinctNode.getSubPlans().size() - 1);
      if (!lastGroupbyNode.isDistinct()) {
        int index = 0;
        for (AggregationFunctionCallEval aggrFunction: lastGroupbyNode.getAggFunctions()) {
          aggrFunction.setIntermediatePhase();
          aggrFunction.setArgs(new EvalNode[]{new FieldEval(lastGroupbyNode.getTargets().get(index).getNamedColumn())});
          index++;
        }
      }
      lastGroupbyNode = thirdStageDistinctNode.getSubPlans().get(thirdStageDistinctNode.getSubPlans().size() - 1);
      if (!lastGroupbyNode.isDistinct()) {
        int index = 0;
        for (AggregationFunctionCallEval aggrFunction: lastGroupbyNode.getAggFunctions()) {
          aggrFunction.setFirstPhase();
          aggrFunction.setArgs(new EvalNode[]{new FieldEval(lastGroupbyNode.getTargets().get(index).getNamedColumn())});
          index++;
        }
      }

      // Set in & out schema for each DistinctGroupbyNode.
      secondStageDistinctNode.setInSchema(firstStageDistinctNode.getOutSchema());
      secondStageDistinctNode.setOutSchema(firstStageDistinctNode.getOutSchema());
      thirdStageDistinctNode.setInSchema(firstStageDistinctNode.getOutSchema());
      thirdStageDistinctNode.setOutSchema(groupbyNode.getOutSchema());

      // Set latestExecBlock's plan with firstDistinctNode
      latestExecBlock.setPlan(firstStageDistinctNode);

      // Make SecondStage ExecutionBlock
      ExecutionBlock secondStageBlock = context.getPlan().newExecutionBlock();

      // Make ThirdStage ExecutionBlock
      ExecutionBlock thirdStageBlock = context.getPlan().newExecutionBlock();

      // Set Enforcer
      setMultiStageAggregationEnforcer(latestExecBlock, firstStageDistinctNode, secondStageBlock,
          secondStageDistinctNode, thirdStageBlock, thirdStageDistinctNode);

      //Create data channel FirstStage to SecondStage
      DataChannel firstChannel = new DataChannel(latestExecBlock, secondStageBlock, HASH_SHUFFLE, 32);

      firstChannel.setShuffleKeys(firstStageDistinctNode.getFirstStageShuffleKeyColumns());
      firstChannel.setSchema(firstStageDistinctNode.getOutSchema());
      firstChannel.setDataFormat(globalPlanner.getDataFormat());

      ScanNode scanNode = GlobalPlanner.buildInputExecutor(context.getPlan().getLogicalPlan(), firstChannel);
      secondStageDistinctNode.setChild(scanNode);

      secondStageBlock.setPlan(secondStageDistinctNode);

      context.getPlan().addConnect(firstChannel);

      DataChannel secondChannel;
      //Create data channel SecondStage to ThirdStage
      if (groupbyNode.isEmptyGrouping()) {
        secondChannel = new DataChannel(secondStageBlock, thirdStageBlock, HASH_SHUFFLE, 1);
        secondChannel.setShuffleKeys(firstStageDistinctNode.getGroupingColumns());
      } else {
        secondChannel = new DataChannel(secondStageBlock, thirdStageBlock, HASH_SHUFFLE, 32);
        secondChannel.setShuffleKeys(firstStageDistinctNode.getGroupingColumns());
      }
      secondChannel.setSchema(secondStageDistinctNode.getOutSchema());
      secondChannel.setDataFormat(globalPlanner.getDataFormat());

      scanNode = GlobalPlanner.buildInputExecutor(context.getPlan().getLogicalPlan(), secondChannel);
      thirdStageDistinctNode.setChild(scanNode);

      thirdStageBlock.setPlan(thirdStageDistinctNode);

      context.getPlan().addConnect(secondChannel);

      if (GlobalPlanner.hasUnionChild(firstStageDistinctNode)) {
        buildDistinctGroupbyAndUnionPlan(
            context.getPlan(), latestExecBlock, firstStageDistinctNode, firstStageDistinctNode);
      }

      return thirdStageBlock;
    } catch (Exception e) {
      throw new TajoInternalError(e);
    }
  }

  private DistinctGroupbyNode buildMultiLevelBaseDistinctGroupByNode(GlobalPlanContext context,
                                                                     ExecutionBlock latestExecBlock,
                                                                     GroupbyNode groupbyNode) {
    /*
     Making DistinctGroupbyNode from GroupByNode
     select col1, count(distinct col2), count(distinct col3), sum(col4) from ... group by col1
     => DistinctGroupbyNode
        Distinct Seq
        grouping key = col1
        Sub GroupbyNodes
         - GroupByNode1: grouping(col2), expr(count distinct col2)
         - GroupByNode2: grouping(col3), expr(count distinct col3)
         - GroupByNode3: expr(sum col4)
    */
    List<Column> originalGroupingColumns = Arrays.asList(groupbyNode.getGroupingColumns());

    List<GroupbyNode> childGroupbyNodes = new ArrayList<>();

    List<AggregationFunctionCallEval> otherAggregationFunctionCallEvals = new ArrayList<>();
    List<Target> otherAggregationFunctionTargets = new ArrayList<>();

    //distinct columns -> GroupbyNode
    Map<String, DistinctGroupbyNodeBuildInfo> distinctNodeBuildInfos = new HashMap<>();
    List<AggregationFunctionCallEval> aggFunctions = groupbyNode.getAggFunctions();
    for (int aggIdx = 0; aggIdx < aggFunctions.size(); aggIdx++) {
      AggregationFunctionCallEval aggFunction = aggFunctions.get(aggIdx);
      aggFunction.setFirstPhase();
      Target originAggFunctionTarget = groupbyNode.getTargets().get(originalGroupingColumns.size() + aggIdx);
      Target aggFunctionTarget =
          new Target(new FieldEval(originAggFunctionTarget.getEvalTree().getName(), aggFunction.getValueType()));

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
          List<Column> groupingColumns = new ArrayList<>();
          for (Column eachGroupingColumn: groupbyUniqColumns) {
            if (!groupingColumns.contains(eachGroupingColumn)) {
              groupingColumns.add(eachGroupingColumn);
            }
          }
          distinctGroupbyNode.setGroupingColumns(groupingColumns.toArray(new Column[groupingColumns.size()]));
        }
        buildInfo.addAggFunction(aggFunction);
        buildInfo.addAggFunctionTarget(aggFunctionTarget);
      } else {
        otherAggregationFunctionCallEvals.add(aggFunction);
        otherAggregationFunctionTargets.add(aggFunctionTarget);
      }
    }

    List<Target> baseGroupByTargets = new ArrayList<>();
    baseGroupByTargets.add(new Target(new FieldEval(new Column("?distinctseq", Type.INT2))));
    for (Column column : originalGroupingColumns) {
      baseGroupByTargets.add(new Target(new FieldEval(column)));
    }

    //Add child groupby node for each Distinct clause
    for (DistinctGroupbyNodeBuildInfo buildInfo: distinctNodeBuildInfos.values()) {
      GroupbyNode eachGroupbyNode = buildInfo.getGroupbyNode();
      List<AggregationFunctionCallEval> groupbyAggFunctions = buildInfo.getAggFunctions();

      List<Target> targets = new ArrayList<>();

      for (Column column : eachGroupbyNode.getGroupingColumns()) {
        Target target = new Target(new FieldEval(column));
        targets.add(target);
        baseGroupByTargets.add(target);
      }
      targets.addAll(buildInfo.getAggFunctionTargets());

      eachGroupbyNode.setTargets(targets);
      eachGroupbyNode.setAggFunctions(groupbyAggFunctions);
      eachGroupbyNode.setDistinct(true);
      eachGroupbyNode.setInSchema(groupbyNode.getInSchema());

      childGroupbyNodes.add(eachGroupbyNode);
    }

    // Merge other aggregation function to a GroupBy Node.
    if (!otherAggregationFunctionCallEvals.isEmpty()) {
      // finally this aggregation output tuple's order is GROUP_BY_COL1, COL2, .... + AGG_VALUE, SUM_VALUE, ...
      GroupbyNode otherGroupbyNode = new GroupbyNode(context.getPlan().getLogicalPlan().newPID());

      List<Target> targets = new ArrayList<>();
      targets.addAll(otherAggregationFunctionTargets);
      baseGroupByTargets.addAll(otherAggregationFunctionTargets);

      otherGroupbyNode.setTargets(targets);
      otherGroupbyNode.setGroupingColumns(new Column[]{});
      otherGroupbyNode.setAggFunctions(otherAggregationFunctionCallEvals);
      otherGroupbyNode.setInSchema(groupbyNode.getInSchema());

      childGroupbyNodes.add(otherGroupbyNode);
    }

    DistinctGroupbyNode baseDistinctNode = new DistinctGroupbyNode(context.getPlan().getLogicalPlan().newPID());
    baseDistinctNode.setTargets(baseGroupByTargets);
    baseDistinctNode.setGroupingColumns(groupbyNode.getGroupingColumns());
    baseDistinctNode.setInSchema(groupbyNode.getInSchema());
    baseDistinctNode.setChild(groupbyNode.getChild());

    baseDistinctNode.setSubPlans(childGroupbyNodes);

    return baseDistinctNode;
  }

  public ExecutionBlock buildPlan(GlobalPlanContext context,
                                  ExecutionBlock latestExecBlock,
                                  LogicalNode currentNode) {
    try {
      GroupbyNode groupbyNode = (GroupbyNode)currentNode;
      LogicalPlan plan = context.getPlan().getLogicalPlan();
      DistinctGroupbyNode baseDistinctNode = buildBaseDistinctGroupByNode(context, latestExecBlock, groupbyNode);

      // Create First, SecondStage's Node using baseNode
      DistinctGroupbyNode[] distinctNodes = createTwoPhaseDistinctNode(plan, groupbyNode, baseDistinctNode);

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
      channel.setDataFormat(globalPlanner.getDataFormat());

      ScanNode scanNode = GlobalPlanner.buildInputExecutor(context.getPlan().getLogicalPlan(), channel);
      secondStageDistinctNode.setChild(scanNode);

      secondStageBlock.setPlan(secondStageDistinctNode);

      context.getPlan().addConnect(channel);

      if (GlobalPlanner.hasUnionChild(firstStageDistinctNode)) {
        buildDistinctGroupbyAndUnionPlan(
            context.getPlan(), latestExecBlock, firstStageDistinctNode, firstStageDistinctNode);
      }

      return secondStageBlock;
    } catch (Exception e) {
      throw new TajoInternalError(e);
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

    List<GroupbyNode> childGroupbyNodes = new ArrayList<>();

    List<AggregationFunctionCallEval> otherAggregationFunctionCallEvals = new ArrayList<>();
    List<Target> otherAggregationFunctionTargets = new ArrayList<>();

    //distinct columns -> GroupbyNode
    Map<String, DistinctGroupbyNodeBuildInfo> distinctNodeBuildInfos = new HashMap<>();

    List<AggregationFunctionCallEval> aggFunctions = groupbyNode.getAggFunctions();
    for (int aggIdx = 0; aggIdx < aggFunctions.size(); aggIdx++) {
      AggregationFunctionCallEval aggFunction = aggFunctions.get(aggIdx);
      Target aggFunctionTarget = groupbyNode.getTargets().get(originalGroupingColumns.size() + aggIdx);

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
          List<Column> groupingColumns = new ArrayList<>(originalGroupingColumns);
          for (Column eachGroupingColumn: groupbyUniqColumns) {
            if (!groupingColumns.contains(eachGroupingColumn)) {
              groupingColumns.add(eachGroupingColumn);
            }
          }
          distinctGroupbyNode.setGroupingColumns(groupingColumns.toArray(new Column[groupingColumns.size()]));
        }
        buildInfo.addAggFunction(aggFunction);
        buildInfo.addAggFunctionTarget(aggFunctionTarget);
      } else {
        aggFunction.setLastPhase();
        otherAggregationFunctionCallEvals.add(aggFunction);
        otherAggregationFunctionTargets.add(aggFunctionTarget);
      }
    }

    //Add child groupby node for each Distinct clause
    for (DistinctGroupbyNodeBuildInfo buildInfo: distinctNodeBuildInfos.values()) {
      GroupbyNode eachGroupbyNode = buildInfo.getGroupbyNode();
      List<AggregationFunctionCallEval> groupbyAggFunctions = buildInfo.getAggFunctions();
      List<Target> targets = new ArrayList<>();

      for (Column column : eachGroupbyNode.getGroupingColumns()) {
        targets.add(new Target(new FieldEval(column)));
      }
      targets.addAll(buildInfo.getAggFunctionTargets());

      eachGroupbyNode.setTargets(targets);
      eachGroupbyNode.setAggFunctions(groupbyAggFunctions);
      eachGroupbyNode.setDistinct(true);
      eachGroupbyNode.setInSchema(groupbyNode.getInSchema());

      childGroupbyNodes.add(eachGroupbyNode);
    }

    // Merge other aggregation function to a GroupBy Node.
    if (!otherAggregationFunctionCallEvals.isEmpty()) {
      // finally this aggregation output tuple's order is GROUP_BY_COL1, COL2, .... + AGG_VALUE, SUM_VALUE, ...
      GroupbyNode otherGroupbyNode = new GroupbyNode(context.getPlan().getLogicalPlan().newPID());

      List<Target> targets = new ArrayList<>();
      for (Column column : originalGroupingColumns) {
        targets.add(new Target(new FieldEval(column)));
      }
      targets.addAll(otherAggregationFunctionTargets);

      otherGroupbyNode.setTargets(targets);
      otherGroupbyNode.setGroupingColumns(originalGroupingColumns.toArray(new Column[originalGroupingColumns.size()]));
      otherGroupbyNode.setAggFunctions(otherAggregationFunctionCallEvals);
      otherGroupbyNode.setInSchema(groupbyNode.getInSchema());

      childGroupbyNodes.add(otherGroupbyNode);
    }

    DistinctGroupbyNode baseDistinctNode = new DistinctGroupbyNode(context.getPlan().getLogicalPlan().newPID());
    baseDistinctNode.setTargets(groupbyNode.getTargets());
    baseDistinctNode.setGroupingColumns(groupbyNode.getGroupingColumns());
    baseDistinctNode.setInSchema(groupbyNode.getInSchema());
    baseDistinctNode.setChild(groupbyNode.getChild());

    baseDistinctNode.setSubPlans(childGroupbyNodes);

    return baseDistinctNode;
  }

  public DistinctGroupbyNode[] createTwoPhaseDistinctNode(LogicalPlan plan,
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
      if (column.hasQualifier()) {
        secondStageColumnIds[originOutputSchema.getColumnId(column.getQualifiedName())] = columnIdIndex;
      } else {
        secondStageColumnIds[originOutputSchema.getColumnIdByName(column.getSimpleName())] = columnIdIndex;
      }
      columnIdIndex++;
    }

    // Split groupby node into two stage.
    // - Remove distinct aggregations from FirstStage.
    // - Change SecondStage's aggregation expr and target column name. For example:
    //     exprs: (sum(default.lineitem.l_quantity (FLOAT8))) ==> exprs: (sum(?sum_3 (FLOAT8)))
    int grpIdx = 0;
    for (GroupbyNode firstStageGroupbyNode: firstStageDistinctNode.getSubPlans()) {
      GroupbyNode secondStageGroupbyNode = secondStageDistinctNode.getSubPlans().get(grpIdx);

      if (firstStageGroupbyNode.isDistinct()) {
        // FirstStage: Remove aggregation, Set target with only grouping columns
        firstStageGroupbyNode.setAggFunctions(PlannerUtil.EMPTY_AGG_FUNCS);

        List<Target> firstGroupbyTargets = new ArrayList<>();
        for (Column column : firstStageGroupbyNode.getGroupingColumns()) {
          Target target = new Target(new FieldEval(column));
          firstGroupbyTargets.add(target);
        }
        firstStageGroupbyNode.setTargets(firstGroupbyTargets);

        // SecondStage:
        //   Set grouping column with origin groupby's columns
        //   Remove distinct group column from targets
        secondStageGroupbyNode.setGroupingColumns(originGroupColumns.toArray(new Column[originGroupColumns.size()]));

        List<Target> oldTargets = secondStageGroupbyNode.getTargets();
        List<Target> secondGroupbyTargets = new ArrayList<>();
        LinkedHashSet<Column> distinctColumns = EvalTreeUtil.findUniqueColumns(secondStageGroupbyNode.getAggFunctions().get(0));
        List<Column> uniqueDistinctColumn = new ArrayList<>();
        // remove origin group by column from distinctColumns
        for (Column eachColumn: distinctColumns) {
          if (!originGroupColumns.contains(eachColumn)) {
            uniqueDistinctColumn.add(eachColumn);
          }
        }
        for (int i = 0; i < originGroupColumns.size(); i++) {
          secondGroupbyTargets.add(oldTargets.get(i));
          if (grpIdx > 0) {
            columnIdIndex++;
          }
        }

        for (int aggFuncIdx = 0; aggFuncIdx < secondStageGroupbyNode.getAggFunctions().size(); aggFuncIdx++) {
          secondStageGroupbyNode.getAggFunctions().get(aggFuncIdx).setLastPhase();
          int targetIdx = originGroupColumns.size() + uniqueDistinctColumn.size() + aggFuncIdx;
          Target aggFuncTarget = oldTargets.get(targetIdx);
          secondGroupbyTargets.add(aggFuncTarget);
          Column column = aggFuncTarget.getNamedColumn();
          if (column.hasQualifier()) {
            secondStageColumnIds[originOutputSchema.getColumnId(column.getQualifiedName())] = columnIdIndex;
          } else {
            secondStageColumnIds[originOutputSchema.getColumnIdByName(column.getSimpleName())] = columnIdIndex;
          }
          columnIdIndex++;
        }
        secondStageGroupbyNode.setTargets(secondGroupbyTargets);
      } else {
        // FirstStage: Change target of aggFunction to function name expr
        List<Target> firstGroupbyTargets = new ArrayList<>();
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

          AggregationFunctionCallEval secondStageAggFunction = secondStageGroupbyNode.getAggFunctions().get(aggFuncIdx);
          secondStageAggFunction.setArgs(new EvalNode[] {firstEval});
          secondStageAggFunction.setLastPhase();

          Target secondTarget = secondStageGroupbyNode.getTargets().get(secondStageGroupbyNode.getGroupingColumns().length + aggFuncIdx);
          Column column = secondTarget.getNamedColumn();
          if (column.hasQualifier()) {
            secondStageColumnIds[originOutputSchema.getColumnId(column.getQualifiedName())] = columnIdIndex;
          } else {
            secondStageColumnIds[originOutputSchema.getColumnIdByName(column.getSimpleName())] = columnIdIndex;
          }
          columnIdIndex++;
          aggFuncIdx++;
        }
        firstStageGroupbyNode.setTargets(firstGroupbyTargets);
        secondStageGroupbyNode.setInSchema(firstStageGroupbyNode.getOutSchema());
      }
      grpIdx++;
    }

    // In the case of distinct query without group by clause
    // other aggregation function is added to last distinct group by node.
    List<GroupbyNode> secondStageGroupbyNodes = secondStageDistinctNode.getSubPlans();
    GroupbyNode lastSecondStageGroupbyNode = secondStageGroupbyNodes.get(secondStageGroupbyNodes.size() - 1);
    if (!lastSecondStageGroupbyNode.isDistinct() && lastSecondStageGroupbyNode.isEmptyGrouping()) {
      GroupbyNode otherGroupbyNode = lastSecondStageGroupbyNode;
      lastSecondStageGroupbyNode = secondStageGroupbyNodes.get(secondStageGroupbyNodes.size() - 2);
      secondStageGroupbyNodes.remove(secondStageGroupbyNodes.size() - 1);

      List<Target> targets = new ArrayList<>();
      targets.addAll(lastSecondStageGroupbyNode.getTargets());
      targets.addAll(otherGroupbyNode.getTargets());

      lastSecondStageGroupbyNode.setTargets(targets);

      List<AggregationFunctionCallEval> aggFunctions = new ArrayList<>();
      aggFunctions.addAll(lastSecondStageGroupbyNode.getAggFunctions());
      aggFunctions.addAll(otherGroupbyNode.getAggFunctions());

      lastSecondStageGroupbyNode.setAggFunctions(aggFunctions);
    }

    // Set FirstStage DistinctNode's target with grouping column and other aggregation function
    List<Integer> firstStageColumnIds = new ArrayList<>();
    columnIdIndex = 0;
    List<Target> firstTargets = new ArrayList<>();
    for (GroupbyNode firstStageGroupbyNode: firstStageDistinctNode.getSubPlans()) {
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
        List<Target> baseGroupbyTargets = firstStageGroupbyNode.getTargets();
        for (int i = firstStageGroupbyNode.getGroupingColumns().length;
             i < baseGroupbyTargets.size(); i++) {
          firstTargets.add(baseGroupbyTargets.get(i));
          firstStageColumnIds.add(columnIdIndex++);
        }
      }
    }
    firstStageDistinctNode.setTargets(firstTargets);
    firstStageDistinctNode.setResultColumnIds(TUtil.toArray(firstStageColumnIds));

    //Set SecondStage ColumnId and Input schema
    secondStageDistinctNode.setResultColumnIds(secondStageColumnIds);

    Schema secondStageInSchema = new Schema();
    //TODO merged tuple schema
    int index = 0;
    for(GroupbyNode eachNode: secondStageDistinctNode.getSubPlans()) {
      eachNode.setInSchema(firstStageDistinctNode.getOutSchema());
      for (Column column: eachNode.getOutSchema().getRootColumns()) {
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

    List<SortSpecArray> sortSpecArrays = new ArrayList<>();
    int index = 0;
    for (GroupbyNode groupbyNode: firstStageDistinctNode.getSubPlans()) {
      List<SortSpecProto> sortSpecs = new ArrayList<>();
      for (Column column: groupbyNode.getGroupingColumns()) {
        sortSpecs.add(SortSpecProto.newBuilder().setColumn(column.getProto()).build());
      }
      sortSpecArrays.add( SortSpecArray.newBuilder()
          .setNodeId(secondStageDistinctNode.getSubPlans().get(index).getPID())
          .addAllSortSpecs(sortSpecs).build());
    }
    secondStageBlock.getEnforcer().enforceDistinctAggregation(secondStageDistinctNode.getPID(),
        DistinctAggregationAlgorithm.SORT_AGGREGATION, sortSpecArrays);

  }

  private void setMultiStageAggregationEnforcer(
      ExecutionBlock firstStageBlock, DistinctGroupbyNode firstStageDistinctNode,
      ExecutionBlock secondStageBlock, DistinctGroupbyNode secondStageDistinctNode,
      ExecutionBlock thirdStageBlock, DistinctGroupbyNode thirdStageDistinctNode) {
    firstStageBlock.getEnforcer().enforceDistinctAggregation(firstStageDistinctNode.getPID(),
        true, MultipleAggregationStage.FIRST_STAGE,
        DistinctAggregationAlgorithm.HASH_AGGREGATION, null);

    secondStageBlock.getEnforcer().enforceDistinctAggregation(secondStageDistinctNode.getPID(),
        true, MultipleAggregationStage.SECOND_STAGE,
        DistinctAggregationAlgorithm.HASH_AGGREGATION, null);

    List<SortSpecArray> sortSpecArrays = new ArrayList<>();
    int index = 0;
    for (GroupbyNode groupbyNode: firstStageDistinctNode.getSubPlans()) {
      List<SortSpecProto> sortSpecs = new ArrayList<>();
      for (Column column: groupbyNode.getGroupingColumns()) {
        sortSpecs.add(SortSpecProto.newBuilder().setColumn(column.getProto()).build());
      }
      sortSpecArrays.add( SortSpecArray.newBuilder()
          .setNodeId(thirdStageDistinctNode.getSubPlans().get(index).getPID())
          .addAllSortSpecs(sortSpecs).build());
    }
    thirdStageBlock.getEnforcer().enforceDistinctAggregation(thirdStageDistinctNode.getPID(),
        true, MultipleAggregationStage.THRID_STAGE,
        DistinctAggregationAlgorithm.SORT_AGGREGATION, sortSpecArrays);
  }

  private ExecutionBlock buildDistinctGroupbyAndUnionPlan(MasterPlan masterPlan, ExecutionBlock lastBlock,
                                                         DistinctGroupbyNode firstPhaseGroupBy,
                                                         DistinctGroupbyNode secondPhaseGroupBy) throws TajoException {
    DataChannel lastDataChannel = null;

    // It pushes down the first phase group-by operator into all child blocks.
    //
    // (second phase)    G (currentBlock)
    //                  /|\
    //                / / | \
    // (first phase) G G  G  G (child block)

    // They are already connected one another.
    // So, we don't need to connect them again.
    for (DataChannel dataChannel : masterPlan.getIncomingChannels(lastBlock.getId())) {
      if (firstPhaseGroupBy.isEmptyGrouping()) {
        dataChannel.setShuffle(HASH_SHUFFLE, firstPhaseGroupBy.getGroupingColumns(), 1);
      } else {
        dataChannel.setShuffle(HASH_SHUFFLE, firstPhaseGroupBy.getGroupingColumns(), 32);
      }
      dataChannel.setSchema(firstPhaseGroupBy.getOutSchema());
      ExecutionBlock childBlock = masterPlan.getExecBlock(dataChannel.getSrcId());

      // Why must firstPhaseGroupby be copied?
      //
      // A groupby in each execution block can have different child.
      // It affects groupby's input schema.
      DistinctGroupbyNode firstPhaseGroupbyCopy = PlannerUtil.clone(masterPlan.getLogicalPlan(), firstPhaseGroupBy);
      firstPhaseGroupbyCopy.setChild(childBlock.getPlan());
      childBlock.setPlan(firstPhaseGroupbyCopy);

      // just keep the last data channel.
      lastDataChannel = dataChannel;
    }

    ScanNode scanNode = GlobalPlanner.buildInputExecutor(masterPlan.getLogicalPlan(), lastDataChannel);
    secondPhaseGroupBy.setChild(scanNode);
    lastBlock.setPlan(secondPhaseGroupBy);
    return lastBlock;
  }

  static class DistinctGroupbyNodeBuildInfo {
    private GroupbyNode groupbyNode;
    private List<AggregationFunctionCallEval> aggFunctions = new ArrayList<>();
    private List<Target> aggFunctionTargets = new ArrayList<>();

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
