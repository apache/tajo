/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *
 */
package tajo.engine.planner;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import tajo.TaskAttemptContext;
import tajo.catalog.Column;
import tajo.catalog.SortSpec;
import tajo.conf.TajoConf;
import tajo.engine.planner.logical.*;
import tajo.engine.planner.physical.*;
import tajo.exception.InternalException;
import tajo.storage.Fragment;
import tajo.storage.StorageManager;
import tajo.storage.TupleComparator;
import tajo.util.IndexUtil;

import java.io.IOException;

public class PhysicalPlannerImpl implements PhysicalPlanner {
  private static final Log LOG = LogFactory.getLog(PhysicalPlannerImpl.class);
  protected final TajoConf conf;
  protected final StorageManager sm;

  public PhysicalPlannerImpl(final TajoConf conf, final StorageManager sm) {
    this.conf = conf;
    this.sm = sm;
  }

  public PhysicalExec createPlan(final TaskAttemptContext context,
      final LogicalNode logicalPlan) throws InternalException {

    PhysicalExec plan;

    try {
      plan = createPlanRecursive(context, logicalPlan);

    } catch (IOException ioe) {
      throw new InternalException(ioe);
    }

    return plan;
  }

  private PhysicalExec createPlanRecursive(TaskAttemptContext ctx, LogicalNode logicalNode) throws IOException {
    PhysicalExec outer;
    PhysicalExec inner;

    switch (logicalNode.getType()) {

      case ROOT:
        LogicalRootNode rootNode = (LogicalRootNode) logicalNode;
        return createPlanRecursive(ctx, rootNode.getSubNode());

      case EXPRS:
        EvalExprNode evalExpr = (EvalExprNode) logicalNode;
        return new EvalExprExec(ctx, evalExpr);

      case STORE:
        StoreTableNode storeNode = (StoreTableNode) logicalNode;
        outer = createPlanRecursive(ctx, storeNode.getSubNode());
        return createStorePlan(ctx, storeNode, outer);

      case SELECTION:
        SelectionNode selNode = (SelectionNode) logicalNode;
        outer = createPlanRecursive(ctx, selNode.getSubNode());
        return new SelectionExec(ctx, selNode, outer);

      case PROJECTION:
        ProjectionNode prjNode = (ProjectionNode) logicalNode;
        outer = createPlanRecursive(ctx, prjNode.getSubNode());
        return new ProjectionExec(ctx, prjNode, outer);

      case SCAN:
        outer = createScanPlan(ctx, (ScanNode) logicalNode);
        return outer;

      case GROUP_BY:
        GroupbyNode grpNode = (GroupbyNode) logicalNode;
        outer = createPlanRecursive(ctx, grpNode.getSubNode());
        return createGroupByPlan(ctx, grpNode, outer);

      case SORT:
        SortNode sortNode = (SortNode) logicalNode;
        outer = createPlanRecursive(ctx, sortNode.getSubNode());
        return createSortPlan(ctx, sortNode, outer);

      case JOIN:
        JoinNode joinNode = (JoinNode) logicalNode;
        outer = createPlanRecursive(ctx, joinNode.getOuterNode());
        inner = createPlanRecursive(ctx, joinNode.getInnerNode());
        return createJoinPlan(ctx, joinNode, outer, inner);

      case UNION:
        UnionNode unionNode = (UnionNode) logicalNode;
        outer = createPlanRecursive(ctx, unionNode.getOuterNode());
        inner = createPlanRecursive(ctx, unionNode.getInnerNode());
        return new UnionExec(ctx, outer, inner);

      case LIMIT:
        LimitNode limitNode = (LimitNode) logicalNode;
        outer = createPlanRecursive(ctx, limitNode.getSubNode());
        return new LimitExec(ctx, limitNode.getInSchema(),
            limitNode.getOutSchema(), outer, limitNode);

      case CREATE_INDEX:
        IndexWriteNode createIndexNode = (IndexWriteNode) logicalNode;
        outer = createPlanRecursive(ctx, createIndexNode.getSubNode());
        return createIndexWritePlan(sm, ctx, createIndexNode, outer);

      case BST_INDEX_SCAN:
        IndexScanNode indexScanNode = (IndexScanNode) logicalNode;
        outer = createIndexScanExec(ctx, indexScanNode);
        return outer;

      case RENAME:
      case SET_UNION:
      case SET_DIFF:
      case SET_INTERSECT:
      case INSERT_INTO:
      case SHOW_TABLE:
      case DESC_TABLE:
      case SHOW_FUNCTION:
      default:
        return null;
    }
  }

  private long estimateSizeRecursive(TaskAttemptContext ctx, String [] tableIds) {
    long size = 0;
    for (String tableId : tableIds) {
      Fragment[] fragments = ctx.getTables(tableId);
      for (Fragment frag : fragments) {
        size += frag.getLength();
      }
    }
    return size;
  }

  public PhysicalExec createJoinPlan(TaskAttemptContext ctx, JoinNode joinNode,
                                     PhysicalExec outer, PhysicalExec inner)
      throws IOException {
    switch (joinNode.getJoinType()) {
      case CROSS_JOIN:
        LOG.info("The planner chooses NLJoinExec");
        return new NLJoinExec(ctx, joinNode, outer, inner);

      case INNER:
        String [] outerLineage = PlannerUtil.getLineage(joinNode.getOuterNode());
        String [] innerLineage = PlannerUtil.getLineage(joinNode.getInnerNode());
        long outerSize = estimateSizeRecursive(ctx, outerLineage);
        long innerSize = estimateSizeRecursive(ctx, innerLineage);

        final long threshold = 1048576 * 128; // 64MB

        boolean hashJoin = false;
        if (outerSize < threshold || innerSize < threshold) {
          hashJoin = true;
        }

        if (hashJoin) {
          PhysicalExec selectedOuter;
          PhysicalExec selectedInner;

          // HashJoinExec loads the inner relation to memory.
          if (outerSize <= innerSize) {
            selectedInner = outer;
            selectedOuter = inner;
          } else {
            selectedInner = inner;
            selectedOuter = outer;
          }

          LOG.info("The planner chooses HashJoinExec");
          return new HashJoinExec(ctx, joinNode, selectedOuter, selectedInner);
        }

      default:
        SortSpec[][] sortSpecs = PlannerUtil.getSortKeysFromJoinQual(
            joinNode.getJoinQual(), outer.getSchema(), inner.getSchema());
        ExternalSortExec outerSort = new ExternalSortExec(ctx, sm,
            new SortNode(sortSpecs[0], outer.getSchema(), outer.getSchema()),
            outer);
        ExternalSortExec innerSort = new ExternalSortExec(ctx, sm,
            new SortNode(sortSpecs[1], inner.getSchema(), inner.getSchema()),
            inner);

        LOG.info("The planner chooses MergeJoinExec");
        return new MergeJoinExec(ctx, joinNode, outerSort, innerSort,
            sortSpecs[0], sortSpecs[1]);
    }
  }

  public PhysicalExec createStorePlan(TaskAttemptContext ctx,
                                      StoreTableNode plan, PhysicalExec subOp) throws IOException {
    if (plan.hasPartitionKey()) {
      switch (plan.getPartitionType()) {
        case HASH:
          return new PartitionedStoreExec(ctx, sm, plan, subOp);

        case RANGE:
          SortSpec [] sortSpecs = null;
          if (subOp instanceof SortExec) {
            sortSpecs = ((SortExec)subOp).getSortSpecs();
          } else {
            Column[] columns = plan.getPartitionKeys();
            SortSpec specs[] = new SortSpec[columns.length];
            for (int i = 0; i < columns.length; i++) {
              specs[i] = new SortSpec(columns[i]);
            }
          }

          return new IndexedStoreExec(ctx, sm, subOp,
              plan.getInSchema(), plan.getInSchema(), sortSpecs);
      }
    }
    if (plan instanceof StoreIndexNode) {
      return new TunnelExec(ctx, plan.getOutSchema(), subOp);
    }

    return new StoreTableExec(ctx, sm, plan, subOp);
  }

  public PhysicalExec createScanPlan(TaskAttemptContext ctx, ScanNode scanNode)
      throws IOException {
    Preconditions.checkNotNull(ctx.getTable(scanNode.getTableId()),
        "Error: There is no table matched to %s", scanNode.getTableId());

    Fragment[] fragments = ctx.getTables(scanNode.getTableId());
    return new SeqScanExec(ctx, sm, scanNode, fragments);
  }

  public PhysicalExec createGroupByPlan(TaskAttemptContext ctx,
                                        GroupbyNode groupbyNode, PhysicalExec subOp) throws IOException {
    Column[] grpColumns = groupbyNode.getGroupingColumns();
    if (grpColumns.length == 0) {
      LOG.info("The planner chooses HashAggregationExec");
      return new HashAggregateExec(ctx, groupbyNode, subOp);
    } else {
      String [] outerLineage = PlannerUtil.getLineage(groupbyNode.getSubNode());
      long estimatedSize = estimateSizeRecursive(ctx, outerLineage);
      final long threshold = 1048576 * 256;

      // if the relation size is less than the reshold,
      // the hash aggregation will be used.
      if (estimatedSize <= threshold) {
        LOG.info("The planner chooses HashAggregationExec");
        return new HashAggregateExec(ctx, groupbyNode, subOp);
      } else {
        SortSpec[] specs = new SortSpec[grpColumns.length];
        for (int i = 0; i < grpColumns.length; i++) {
          specs[i] = new SortSpec(grpColumns[i], true, false);
        }
        SortNode sortNode = new SortNode(specs);
        sortNode.setInSchema(subOp.getSchema());
        sortNode.setOutSchema(subOp.getSchema());
        // SortExec sortExec = new SortExec(sortNode, child);
        ExternalSortExec sortExec = new ExternalSortExec(ctx, sm, sortNode,
            subOp);
        LOG.info("The planner chooses SortAggregationExec");
        return new SortAggregateExec(ctx, groupbyNode, sortExec);
      }
    }
  }

  public PhysicalExec createSortPlan(TaskAttemptContext ctx, SortNode sortNode,
                                     PhysicalExec subOp) throws IOException {
    return new ExternalSortExec(ctx, sm, sortNode, subOp);
  }

  public PhysicalExec createIndexWritePlan(StorageManager sm,
                                           TaskAttemptContext ctx, IndexWriteNode indexWriteNode, PhysicalExec subOp)
      throws IOException {

    return new IndexWriteExec(ctx, sm, indexWriteNode, ctx.getTable(indexWriteNode
        .getTableName()), subOp);
  }

  public PhysicalExec createIndexScanExec(TaskAttemptContext ctx,
                                          IndexScanNode annotation)
      throws IOException {
    //TODO-general Type Index
    Preconditions.checkNotNull(ctx.getTable(annotation.getTableId()),
        "Error: There is no table matched to %s", annotation.getTableId());

    Fragment[] fragments = ctx.getTables(annotation.getTableId());

    String indexName = IndexUtil.getIndexNameOfFrag(fragments[0],
        annotation.getSortKeys());
    Path indexPath = new Path(sm.getTablePath(annotation.getTableId()), "index");

    TupleComparator comp = new TupleComparator(annotation.getKeySchema(),
        annotation.getSortKeys());
    return new BSTIndexScanExec(ctx, sm, annotation, fragments[0], new Path(
        indexPath, indexName), annotation.getKeySchema(), comp,
        annotation.getDatum());

  }
}
