package tajo.engine.exec.eval;

/**
 * @author Hyunsik Choi
 */
public interface EvalNodeVisitor {
  public void visit(EvalNode node);
}
