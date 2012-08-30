package tajo.engine.planner.physical;

import com.google.common.collect.Sets;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.datum.DatumFactory;
import tajo.SubqueryContext;
import tajo.engine.exec.eval.ConstEval;
import tajo.engine.exec.eval.EvalContext;
import tajo.engine.exec.eval.EvalNode;
import tajo.engine.parser.QueryBlock;
import tajo.engine.planner.logical.GroupbyNode;

import java.io.IOException;
import java.util.Set;

/**
 * @author Hyunsik Choi
 */
public abstract class AggregationExec extends PhysicalExec {
  protected final SubqueryContext ctx;
  protected GroupbyNode annotation;
  protected PhysicalExec child;

  @SuppressWarnings("unused")
  protected final EvalNode havingQual;
  protected final Schema inputSchema;
  protected final Schema outputSchema;

  protected Set<Column> nonNullGroupingFields;
  protected int keylist [];
  protected int measureList[];
  protected final EvalNode evals [];
  protected EvalContext evalContexts [];

  public AggregationExec(SubqueryContext ctx, GroupbyNode groupbyNode,
                           PhysicalExec child) throws IOException {
    this.ctx = ctx;
    this.annotation = groupbyNode;
    this.havingQual = groupbyNode.getHavingCondition();
    this.inputSchema = groupbyNode.getInputSchema();
    this.outputSchema = groupbyNode.getOutputSchema();
    this.child = child;

    nonNullGroupingFields = Sets.newHashSet();
    // keylist will contain a list of IDs of grouping column
    keylist = new int[groupbyNode.getGroupingColumns().length];
    Column col;
    for (int idx = 0; idx < groupbyNode.getGroupingColumns().length; idx++) {
      col = groupbyNode.getGroupingColumns()[idx];
      keylist[idx] = inputSchema.getColumnId(col.getQualifiedName());
      nonNullGroupingFields.add(col);
    }

    // measureList will contain a list of IDs of measure fields
    int valueIdx = 0;
    measureList = new int[groupbyNode.getTargets().length - keylist.length];
    if (measureList.length > 0) {
      search: for (int inputIdx = 0; inputIdx < groupbyNode.getTargets().length; inputIdx++) {
        for (int key : keylist) { // eliminate key field
          if (groupbyNode.getTargets()[inputIdx].getColumnSchema().getColumnName()
              .equals(inputSchema.getColumn(key).getColumnName())) {
            continue search;
          }
        }
        measureList[valueIdx] = inputIdx;
        valueIdx++;
      }
    }

    evals = new EvalNode[groupbyNode.getTargets().length];
    evalContexts = new EvalContext[groupbyNode.getTargets().length];
    for (int i = 0; i < groupbyNode.getTargets().length; i++) {
      QueryBlock.Target t = groupbyNode.getTargets()[i];
      if (t.getEvalTree().getType() == EvalNode.Type.FIELD && !nonNullGroupingFields.contains(t.getColumnSchema())) {
        evals[i] = new ConstEval(DatumFactory.createNullDatum());
        evalContexts[i] = evals[i].newContext();
      } else {
        evals[i] = t.getEvalTree();
        evalContexts[i] = evals[i].newContext();
      }
    }
  }
}
