package nta.engine.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import nta.engine.exec.eval.EvalNode.Type;
import nta.engine.exec.eval.EvalTreeUtil;
import nta.engine.exec.eval.FuncCallEval;
import nta.engine.parser.QueryBlock.Target;
import nta.engine.planner.logical.ExprType;
import nta.engine.planner.logical.GroupbyNode;
import nta.engine.planner.logical.LogicalNode;
import nta.engine.planner.logical.LogicalNodeVisitor;
import nta.engine.planner.logical.UnaryNode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;

/**
 * @author Hyunsik Choi
 */
public class PlannerUtil {
  private static final Log LOG = LogFactory.getLog(PlannerUtil.class);
  
  public static LogicalNode insertNode(LogicalNode parent, LogicalNode newNode) {
    Preconditions.checkArgument(parent instanceof UnaryNode);
    Preconditions.checkArgument(newNode instanceof UnaryNode);
    
    UnaryNode p = (UnaryNode) parent;
    UnaryNode c = (UnaryNode) p.getSubNode();
    UnaryNode m = (UnaryNode) newNode;
    m.setInputSchema(c.getOutputSchema());
    m.setOutputSchema(c.getOutputSchema());
    m.setSubNode(c);
    p.setSubNode(m);
    
    return p;
  }
  
  public static LogicalNode transformTwoPhase(GroupbyNode gp) {
    Preconditions.checkNotNull(gp);
        
    try {
      GroupbyNode child = (GroupbyNode) gp.clone();
      gp.setSubNode(child);
      gp.setInputSchema(child.getOutputSchema());
      gp.setOutputSchema(child.getOutputSchema());
    
      Target [] targets = gp.getTargetList();
      for (int i = 0; i < gp.getTargetList().length; i++) {
        if (targets[i].getEvalTree().getType() == Type.FUNCTION) {
          String name = child.getOutputSchema().getColumn(i).getQualifiedName();        
          FuncCallEval eval = (FuncCallEval) targets[i].getEvalTree();
          Collection<String> tobeChanged = EvalTreeUtil.findAllRefColumns(eval);
          EvalTreeUtil.changeColumnRef(eval, tobeChanged.iterator().next(), name);
        }
      }
      
    } catch (CloneNotSupportedException e) {
      LOG.error(e);
    }
    
    return gp;
  }
  
  /**
   * Find the top node of the given plan
   * 
   * @param plan
   * @param type to find
   * @return a found logical node
   */
  public static LogicalNode findTopNode(LogicalNode plan, ExprType type) {
    Preconditions.checkNotNull(plan);
    Preconditions.checkNotNull(type);
    
    LogicalNodeFinder finder = new LogicalNodeFinder(type);
    plan.accept(finder);
    
    if (finder.getFoundNodes().size() == 0) {
      return null;
    }
    return finder.getFoundNodes().get(0);
  }
  
  public static class LogicalNodeFinder implements LogicalNodeVisitor {
    private List<LogicalNode> list = new ArrayList<LogicalNode>();
    private ExprType tofind;

    public LogicalNodeFinder(ExprType type) {
      this.tofind = type;
    }

    @Override
    public void visit(LogicalNode node) {
      if (node.getType() == tofind) {
        list.add(node);
      }
    }

    public List<LogicalNode> getFoundNodes() {
      return list;
    }
  }
}
