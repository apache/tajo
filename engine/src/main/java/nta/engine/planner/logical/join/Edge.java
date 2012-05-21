package nta.engine.planner.logical.join;

import nta.engine.exec.eval.EvalNode;

/**
 * @author
 */
public class Edge {
  private String src;
  private String target;
  private EvalNode joinQual;

  public Edge(String src, String target, EvalNode joinQual) {
    this.src = src;
    this.target = target;
    this.joinQual = joinQual;
  }

  public String getSrc() {
    return this.src;
  }

  public String getTarget() {
    return this.target;
  }

  public EvalNode getJoinQual() {
    return this.joinQual;
  }

  @Override
  public String toString() {
    return "(" + src + "=> " + target + ", " + joinQual + ")";
  }
}
