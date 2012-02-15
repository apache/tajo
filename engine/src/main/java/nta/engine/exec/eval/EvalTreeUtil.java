package nta.engine.exec.eval;

import java.util.HashSet;
import java.util.Set;

import nta.engine.exec.eval.EvalNode.Type;

/**
 * @author Hyunsik Choi
 */
public class EvalTreeUtil {
  public static void changeColumnRef(EvalNode node, String oldName, String newName) {
    node.accept(new ChangeColumnRefVisitor(oldName, newName));
  }
  
  public static Set<String> findAllRefColumns(EvalNode node) {
    AllColumnRefFinder finder = new AllColumnRefFinder();
    node.accept(finder);
    return finder.getAllRefColumns();
  }
  
  public static class ChangeColumnRefVisitor implements EvalNodeVisitor {    
    private final String findColumn;
    private final String toBeChanged;
    
    public ChangeColumnRefVisitor(String oldName, String newName) {
      this.findColumn = oldName;
      this.toBeChanged = newName;
    }
    
    @Override
    public void visit(EvalNode node) {
      if (node.type == Type.FIELD) {
        FieldEval field = (FieldEval) node;
        if (field.getName().equals(findColumn) || 
            field.getColumnName().equals(findColumn)) {
          field.replaceColumnRef(toBeChanged);
        }
      }
    }    
  }
  
  public static class AllColumnRefFinder implements EvalNodeVisitor {
    private Set<String> colList = new HashSet<String>(); 
    private FieldEval field = null;
    
    @Override
    public void visit(EvalNode node) {
      if (node.getType() == Type.FIELD) {
        field = (FieldEval) node;
        colList.add(field.getName());
      }
    }
    
    public Set<String> getAllRefColumns() {
      return this.colList;
    }
  }
}
