package nta.engine.planner.physical;

import com.google.common.collect.Sets;
import nta.catalog.Column;
import nta.catalog.Schema;
import nta.datum.DatumFactory;
import nta.engine.SubqueryContext;
import nta.engine.exec.eval.ConstEval;
import nta.engine.exec.eval.EvalContext;
import nta.engine.exec.eval.EvalNode;
import nta.engine.parser.QueryBlock;
import nta.engine.planner.logical.GroupbyNode;

import java.io.IOException;
import java.util.Set;

/**
 * @author Hyunsik Choi
 */
public abstract class AggregationExec extends PhysicalExec {
  protected final SubqueryContext ctx;
  protected GroupbyNode annotation;
  protected PhysicalExec subOp;

  @SuppressWarnings("unused")
  protected final EvalNode havingQual;
  protected final Schema inputSchema;
  protected final Schema outputSchema;

  protected Set<Column> nonNullGroupingFields;
  protected int keylist [];
  protected int measurelist [];
  protected final EvalNode evals [];
  protected EvalContext evalContexts [];

  public AggregationExec(SubqueryContext ctx, GroupbyNode annotation,
                           PhysicalExec subOp) throws IOException {
    this.ctx = ctx;
    this.annotation = annotation;
    this.havingQual = annotation.getHavingCondition();
    this.inputSchema = annotation.getInputSchema();
    this.outputSchema = annotation.getOutputSchema();
    this.subOp = subOp;

    nonNullGroupingFields = Sets.newHashSet();
    // getting key list
    keylist = new int[annotation.getGroupingColumns().length];
    Column col;
    for (int idx = 0; idx < annotation.getGroupingColumns().length; idx++) {
      col = annotation.getGroupingColumns()[idx];
      keylist[idx] = inputSchema.getColumnId(col.getQualifiedName());
      nonNullGroupingFields.add(col);
    }

    // getting value list
    int valueIdx = 0;
    measurelist = new int[annotation.getTargetList().length - keylist.length];
    if (measurelist.length > 0) {
      search: for (int inputIdx = 0; inputIdx < annotation.getTargetList().length; inputIdx++) {
        for (int key : keylist) { // eliminate key field
          if (annotation.getTargetList()[inputIdx].getColumnSchema().getColumnName()
              .equals(inputSchema.getColumn(key).getColumnName())) {
            continue search;
          }
        }
        measurelist[valueIdx] = inputIdx;
        valueIdx++;
      }
    }

    evals = new EvalNode[annotation.getTargetList().length];
    evalContexts = new EvalContext[annotation.getTargetList().length];
    for (int i = 0; i < annotation.getTargetList().length; i++) {
      QueryBlock.Target t = annotation.getTargetList()[i];
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
