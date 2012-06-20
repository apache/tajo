package nta.engine.planner;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.engine.exec.eval.EvalContext;
import nta.engine.exec.eval.EvalNode;
import nta.engine.parser.QueryBlock.Target;
import nta.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class Projector {
  private final Schema inSchema;
  private final Schema outSchema;

  // for projection
  private final int targetNum;
  private final int [] inMap;
  private final int [] outMap;
  private int [] evalOutMap; // target list?
  private EvalNode[] evals;
  private Tuple prevTuple;

  public Projector(Schema inSchema, Schema outSchema, Target [] targets) {
    this.inSchema = inSchema;
    this.outSchema = outSchema;

    this.targetNum = targets != null ? targets.length : 0;

    inMap = new int[outSchema.getColumnNum() - targetNum];
    outMap = new int[outSchema.getColumnNum() - targetNum];
    int mapId = 0;
    Column col;

    if (targetNum > 0) {
      evalOutMap = new int[targetNum];
      evals = new EvalNode[targetNum];
      for (int i = 0; i < targetNum; i++) {
        // TODO - is it always  correct?
        if (targets[i].hasAlias()) {
          evalOutMap[i] = outSchema.getColumnId(targets[i].getAlias());
        } else {
          evalOutMap[i] = outSchema.getColumnId(targets[i].getEvalTree().getName());
        }
        evals[i] = targets[i].getEvalTree();
      }

      outer:
      for (int targetId = 0; targetId < outSchema.getColumnNum(); targetId ++) {
        for (int j = 0; j < evalOutMap.length; j++) {
          if (evalOutMap[j] == targetId)
            continue outer;
        }

        col = inSchema.getColumn(outSchema.getColumn(targetId).getQualifiedName());
        outMap[mapId] = targetId;
        inMap[mapId] = inSchema.getColumnId(col.getQualifiedName());
        mapId++;
      }
    } else {
      for (int targetId = 0; targetId < outSchema.getColumnNum(); targetId ++) {
        col = inSchema.getColumn(outSchema.getColumn(targetId).getQualifiedName());
        outMap[mapId] = targetId;
        inMap[mapId] = inSchema.getColumnId(col.getQualifiedName());
        mapId++;
      }
    }
  }

  public void eval(EvalContext [] evalContexts, Tuple in) {
    this.prevTuple = in;
    if (targetNum > 0) {
      for (int i = 0; i < evals.length; i++) {
        evals[i].eval(evalContexts[i], inSchema, in);
      }
    }
  }

  public void terminate(EvalContext [] evalContexts, Tuple out) {
    for (int i = 0; i < inMap.length; i++) {
      out.put(outMap[i], prevTuple.get(inMap[i]));
    }
    if (targetNum > 0) {
      for (int i = 0; i < evals.length; i++) {
        out.put(evalOutMap[i], evals[i].terminate(evalContexts[i]));
      }
    }
  }

  public EvalContext [] renew() {
    EvalContext [] evalContexts = new EvalContext[targetNum];
    for (int i = 0; i < targetNum; i++) {
      evalContexts[i] = evals[i].newContext();
    }

    return evalContexts;
  }
}
