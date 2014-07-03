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

package org.apache.tajo.engine.planner.rewrite;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.planner.BasicLogicalPlanVisitor;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.utils.TupleUtil;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public class PartitionedTableRewriter implements RewriteRule {
  private static final Log LOG = LogFactory.getLog(PartitionedTableRewriter.class);

  private static final String NAME = "Partitioned Table Rewriter";
  private final Rewriter rewriter = new Rewriter();

  private final TajoConf systemConf;

  public PartitionedTableRewriter(TajoConf conf) {
    systemConf = conf;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(LogicalPlan plan) {
    for (LogicalPlan.QueryBlock block : plan.getQueryBlocks()) {
      for (RelationNode relation : block.getRelations()) {
        if (relation.getType() == NodeType.SCAN) {
          TableDesc table = ((ScanNode)relation).getTableDesc();
          if (table.hasPartition()) {
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    LogicalPlan.QueryBlock rootBlock = plan.getRootBlock();
    rewriter.visit(rootBlock, plan, rootBlock, rootBlock.getRoot(), new Stack<LogicalNode>());
    return plan;
  }

  private static class PartitionPathFilter implements PathFilter {
    private Schema schema;
    private EvalNode partitionFilter;


    public PartitionPathFilter(Schema schema, EvalNode partitionFilter) {
      this.schema = schema;
      this.partitionFilter = partitionFilter;
    }

    @Override
    public boolean accept(Path path) {
      Tuple tuple = TupleUtil.buildTupleFromPartitionPath(schema, path, true);
      if (tuple == null) { // if it is a file or not acceptable file
        return false;
      }

      return partitionFilter.eval(schema, tuple).asBool();
    }

    @Override
    public String toString() {
      return partitionFilter.toString();
    }
  }

  /**
   * It assumes that each conjunctive form corresponds to one column.
   *
   * @param partitionColumns
   * @param conjunctiveForms search condition corresponding to partition columns.
   *                         If it is NULL, it means that there is no search condition for this table.
   * @param tablePath
   * @return
   * @throws IOException
   */
  private Path [] findFilteredPaths(Schema partitionColumns, EvalNode [] conjunctiveForms, Path tablePath)
      throws IOException {

    FileSystem fs = tablePath.getFileSystem(systemConf);

    PathFilter [] filters;
    if (conjunctiveForms == null) {
      filters = buildAllAcceptingPathFilters(partitionColumns);
    } else {
      filters = buildPathFiltersForAllLevels(partitionColumns, conjunctiveForms);
    }

    // loop from one to the number of partition columns
    Path [] filteredPaths = toPathArray(fs.listStatus(tablePath, filters[0]));

    for (int i = 1; i < partitionColumns.size(); i++) {
      // Get all file status matched to a ith level path filter.
      filteredPaths = toPathArray(fs.listStatus(filteredPaths, filters[i]));
    }

    LOG.info("Filtered directory or files: " + filteredPaths.length);
    return filteredPaths;
  }

  /**
   * Build path filters for all levels with a list of filter conditions.
   *
   * For example, consider you have a partitioned table for three columns (i.e., col1, col2, col3).
   * Then, this methods will create three path filters for (col1), (col1, col2), (col1, col2, col3).
   *
   * Corresponding filter conditions will be placed on each path filter,
   * If there is no corresponding expression for certain column,
   * The condition will be filled with a true value.
   *
   * Assume that an user gives a condition WHERE col1 ='A' and col3 = 'C'.
   * There is no filter condition corresponding to col2.
   * Then, the path filter conditions are corresponding to the followings:
   *
   * The first path filter: col1 = 'A'
   * The second path filter: col1 = 'A' AND col2 IS NOT NULL
   * The third path filter: col1 = 'A' AND col2 IS NOT NULL AND col3 = 'C'
   *
   * 'IS NOT NULL' predicate is always true against the partition path.
   *
   * @param partitionColumns
   * @param conjunctiveForms
   * @return
   */
  private static PathFilter [] buildPathFiltersForAllLevels(Schema partitionColumns,
                                                     EvalNode [] conjunctiveForms) {
    // Building partition path filters for all levels
    Column target;
    PathFilter [] filters = new PathFilter[partitionColumns.size()];
    List<EvalNode> accumulatedFilters = Lists.newArrayList();
    for (int i = 0; i < partitionColumns.size(); i++) { // loop from one to level
      target = partitionColumns.getColumn(i);

      for (EvalNode expr : conjunctiveForms) {
        if (EvalTreeUtil.findUniqueColumns(expr).contains(target)) {
          // Accumulate one qual per level
          accumulatedFilters.add(expr);
        }
      }

      if (accumulatedFilters.size() < (i + 1)) {
        accumulatedFilters.add(new IsNullEval(true, new FieldEval(target)));
      }

      EvalNode filterPerLevel = AlgebraicUtil.createSingletonExprFromCNF(
          accumulatedFilters.toArray(new EvalNode[accumulatedFilters.size()]));
      filters[i] = new PartitionPathFilter(partitionColumns, filterPerLevel);
    }
    return filters;
  }

  /**
   * Build an array of path filters for all levels with all accepting filter condition.
   * @param partitionColumns The partition columns schema
   * @return The array of path filter, accpeting all partition paths.
   */
  private static PathFilter [] buildAllAcceptingPathFilters(Schema partitionColumns) {
    Column target;
    PathFilter [] filters = new PathFilter[partitionColumns.size()];
    List<EvalNode> accumulatedFilters = Lists.newArrayList();
    for (int i = 0; i < partitionColumns.size(); i++) { // loop from one to level
      target = partitionColumns.getColumn(i);
      accumulatedFilters.add(new IsNullEval(true, new FieldEval(target)));

      EvalNode filterPerLevel = AlgebraicUtil.createSingletonExprFromCNF(
          accumulatedFilters.toArray(new EvalNode[accumulatedFilters.size()]));
      filters[i] = new PartitionPathFilter(partitionColumns, filterPerLevel);
    }
    return filters;
  }

  private static Path [] toPathArray(FileStatus[] fileStatuses) {
    Path [] paths = new Path[fileStatuses.length];
    for (int j = 0; j < fileStatuses.length; j++) {
      paths[j] = fileStatuses[j].getPath();
    }
    return paths;
  }

  private Path [] findFilteredPartitionPaths(ScanNode scanNode) throws IOException {
    TableDesc table = scanNode.getTableDesc();
    PartitionMethodDesc partitionDesc = scanNode.getTableDesc().getPartitionMethod();

    Schema paritionValuesSchema = new Schema();
    for (Column column : partitionDesc.getExpressionSchema().getColumns()) {
      paritionValuesSchema.addColumn(column);
    }

    Set<EvalNode> indexablePredicateSet = Sets.newHashSet();

    // if a query statement has a search condition, try to find indexable predicates
    if (scanNode.hasQual()) {
      EvalNode [] conjunctiveForms = AlgebraicUtil.toConjunctiveNormalFormArray(scanNode.getQual());
      Set<EvalNode> remainExprs = Sets.newHashSet(conjunctiveForms);

      // add qualifier to schema for qual
      paritionValuesSchema.setQualifier(scanNode.getCanonicalName());
      for (Column column : paritionValuesSchema.getColumns()) {
        for (EvalNode simpleExpr : conjunctiveForms) {
          if (checkIfIndexablePredicateOnTargetColumn(simpleExpr, column)) {
            indexablePredicateSet.add(simpleExpr);
          }
        }
      }

      // Partitions which are not matched to the partition filter conditions are pruned immediately.
      // So, the partition filter conditions are not necessary later, and they are removed from
      // original search condition for simplicity and efficiency.
      remainExprs.removeAll(indexablePredicateSet);
      if (remainExprs.isEmpty()) {
        scanNode.setQual(null);
      } else {
        scanNode.setQual(
            AlgebraicUtil.createSingletonExprFromCNF(remainExprs.toArray(new EvalNode[remainExprs.size()])));
      }
    }

    if (indexablePredicateSet.size() > 0) { // There are at least one indexable predicates
      return findFilteredPaths(paritionValuesSchema,
          indexablePredicateSet.toArray(new EvalNode[indexablePredicateSet.size()]), table.getPath());
    } else { // otherwise, we will get all partition paths.
      return findFilteredPaths(paritionValuesSchema, null, table.getPath());
    }
  }

  private boolean checkIfIndexablePredicateOnTargetColumn(EvalNode evalNode, Column targetColumn) {
    if (checkIfIndexablePredicate(evalNode) || checkIfDisjunctiveButOneVariable(evalNode)) {
      Set<Column> variables = EvalTreeUtil.findUniqueColumns(evalNode);
      // if it contains only single variable matched to a target column
      return variables.size() == 1 && variables.contains(targetColumn);
    } else {
      return false;
    }
  }

  /**
   * Check if an expression consists of one variable and one constant and
   * the expression is a comparison operator.
   *
   * @param evalNode The expression to be checked
   * @return true if an expression consists of one variable and one constant
   * and the expression is a comparison operator. Other, false.
   */
  private boolean checkIfIndexablePredicate(EvalNode evalNode) {
    // TODO - LIKE with a trailing wild-card character and IN with an array can be indexable
    return AlgebraicUtil.containSingleVar(evalNode) && AlgebraicUtil.isIndexableOperator(evalNode);
  }

  /**
   *
   * @param evalNode The expression to be checked
   * @return true if an disjunctive expression, consisting of indexable expressions
   */
  private boolean checkIfDisjunctiveButOneVariable(EvalNode evalNode) {
    if (evalNode.getType() == EvalType.OR) {
      BinaryEval orEval = (BinaryEval) evalNode;
      boolean indexable =
          checkIfIndexablePredicate(orEval.getLeftExpr()) &&
              checkIfIndexablePredicate(orEval.getRightExpr());

      boolean sameVariable =
          EvalTreeUtil.findUniqueColumns(orEval.getLeftExpr())
          .equals(EvalTreeUtil.findUniqueColumns(orEval.getRightExpr()));

      return indexable && sameVariable;
    } else {
      return false;
    }
  }

  private void updateTableStat(PartitionedTableScanNode scanNode) throws PlanningException {
    if (scanNode.getInputPaths().length > 0) {
      try {
        FileSystem fs = scanNode.getInputPaths()[0].getFileSystem(systemConf);
        long totalVolume = 0;

        for (Path input : scanNode.getInputPaths()) {
          ContentSummary summary = fs.getContentSummary(input);
          totalVolume += summary.getLength();
          totalVolume += summary.getFileCount();
        }
        scanNode.getTableDesc().getStats().setNumBytes(totalVolume);
      } catch (IOException e) {
        throw new PlanningException(e);
      }
    }
  }

  private final class Rewriter extends BasicLogicalPlanVisitor<Object, Object> {
    @Override
    public Object visitScan(Object object, LogicalPlan plan, LogicalPlan.QueryBlock block, ScanNode scanNode,
                            Stack<LogicalNode> stack) throws PlanningException {

      TableDesc table = scanNode.getTableDesc();
      if (!table.hasPartition()) {
        return null;
      }

      try {
        Path [] filteredPaths = findFilteredPartitionPaths(scanNode);
        plan.addHistory("PartitionTableRewriter chooses " + filteredPaths.length + " of partitions");
        PartitionedTableScanNode rewrittenScanNode = plan.createNode(PartitionedTableScanNode.class);
        rewrittenScanNode.init(scanNode, filteredPaths);
        updateTableStat(rewrittenScanNode);

        // if it is topmost node, set it as the rootnode of this block.
        if (stack.empty() || block.getRoot().equals(scanNode)) {
          block.setRoot(rewrittenScanNode);
        } else {
          PlannerUtil.replaceNode(plan, stack.peek(), scanNode, rewrittenScanNode);
        }
      } catch (IOException e) {
        throw new PlanningException("Partitioned Table Rewrite Failed: \n" + e.getMessage());
      }
      return null;
    }
  }
}
